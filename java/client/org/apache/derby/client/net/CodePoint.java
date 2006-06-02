/*

   Derby - Class org.apache.derby.client.net.CodePoint

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

package org.apache.derby.client.net;


public class CodePoint {
    public static final int FIXED_ROW_QUERY_PROTOCOL = 0;
    public static final int LIMITED_BLOCK_QUERY_PROTOCOL = 1;
    public static final int FORCE_FIXED_ROW_QUERY_PROTOCOL = 2;


    // Character Subtype MBCS
    static final int CSTMBCS = 0x2435;


    // Force Fixed Row Query Protocol
    static final int FRCFIXROW = 0x2410;

    // Title
    static final int TITLE = 0x0045;

    // ---------------callable statement codepoints-------------------------------

    // PKGSNLST
    static final int PKGSNLST = 0x2139;

    // Output Expected
    static final int OUTEXP = 0x2111;

    // Procedure Name
    static final int PRCNAM = 0x2138;

    // Maximum Result Set Count.
    static final int MAXRSLCNT = 0x2140;

    // Maximum Result Set Count No Limit.
    // Requester is capable of receiving all result sets in the response to EXCSQLSTT.
    static final int MAXRSLCNT_NOLIMIT = 0xffff;

    // Result Set Flags
    static final int RSLSETFLG = 0x2142;

    static final int RSLSETFLG_RETURN_NAMES = 0x80;

    static final int RSLSETFLG_RETURN_LABELS = 0x40;

    // RSLSETFLGs added in SQLAM 7 for requesting standard, extended, or light sqldas
    static final int RSLSETFLG_STANDARD_SQLDA = 0x00;
    static final int RSLSETFLG_EXTENDED_SQLDA = 0x04;

    // --------------------code points for constant ddm data----------------------

    // Indicates false state.  This 1-byte code point is used by some DDM parameters.
    static final byte FALSE = -16;  // was 0xf0

    // Indicates true state.  This 1-byte code point is used by some DDM parameters.
    static final byte TRUE = -15;  // was 0xf1

    // Zero indicator constant.
    // Indicates data does flow.
    static final int ZEROIND = 0x00;

    // FDOCA NULL indicator constant.
    // Indicates data does not flow.
    static final int NULLDATA = 0xFF;

    // Security check was successful.
    static final int SECCHKCD_00 = 0x00;

    // SECMEC value not supported.
    static final int SECCHKCD_01 = 0x01;

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

    // SQL Error Diagnostic Level
    // DIAGLVL0 A null SQLDIAGGRP is returned. This is the default.
    // DIAGLVL1 A non-null SQLDIAGGRP should be returned.
    // DIAGLVL2 A non-null SQLDIAGGRP should be returned, and both SQLDCMSG
    // message text fields should be returned as null strings.
    static final byte DIAGLVL0 = (byte)0xF0;
    static final byte DIAGLVL1 = (byte)0xF1;
    static final byte DIAGLVL2 = (byte)0xF2;

    // ----------------------ddm code points--------------------------------------

    // Exchange Server Attributes.
    final static int EXCSAT = 0x1041;


    // Sync Point Control Request.
    public final static int SYNCCTL = 0x1055;

    // Sync Point Resync Command.
    final static int SYNCRSY = 0x1069;

    // Access Security.
    final static int ACCSEC = 0x106D;

    // Security Check.
    final static int SECCHK = 0x106E;

    // Access RDB.
    final static int ACCRDB = 0x2001;

    // Close Query.
    final static int CLSQRY = 0x2005;

    // Continue Query.
    final static int CNTQRY = 0x2006;


    // Describe SQL Statement.
    final static int DSCSQLSTT = 0x2008;


    // Execute Immediate SQL Statement.
    final static int EXCSQLIMM = 0x200A;

    // Execute SQL Statement.
    final static int EXCSQLSTT = 0x200B;

    // Set SQL Environment.
    final static int EXCSQLSET = 0x2014;

    // Open Query.
    final static int OPNQRY = 0x200C;

    // Output override.
    final static int OUTOVR = 0x2415;

    // Prepare SQL Statement.
    final static int PRPSQLSTT = 0x200D;

    // RDB Commit Unit of Work.
    final static int RDBCMM = 0x200E;

    // RDB Rollback Unit of Work.
    final static int RDBRLLBCK = 0x200F;


    // Describe RDB Table.
    final static int DSCRDBTBL = 0x2012;

    // SQL Program Variable Data.
    final static int SQLDTA = 0x2412;

    // SQL Data Reply Data.
    public final static int SQLDTARD = 0x2413;

    // SQL Statement.
    final static int SQLSTT = 0x2414;


    // Query Answer Set Description.
    public final static int QRYDSC = 0x241A;

    // Query Answer Set Data.
    public final static int QRYDTA = 0x241B;

    // SQL Statement Attributes.
    final static int SQLATTR = 0x2450;

    // Access Security Reply Data.
    // Contains the security information from a target server's
    // security manager.  This information is returned in response
    // to an ACCSEC command.
    static final int ACCSECRD = 0x14AC;


    // Agent codepoint constant.
    static final int AGENT = 0x1403;

    // The codepoint for codepoint
    static final int CODPNT = 0x000C;

    // CCSID for Double-Byte Characters codepoint constant.
    static final int CCSIDDBC = 0x119D;

    // CCSID for Mixed-Byte Characters codepoint constant.
    static final int CCSIDMBC = 0x119E;


    // CCSID for Single-Byte Characters codepoint constant.
    static final int CCSIDSBC = 0x119C;

    // Describes the communications manager that supports
    // conversational protocols by using System Network
    // Architecture Logical Unit 6.2 (SNA LU 6.2) local
    // communications facilities.
    static final int CMNAPPC = 0x1444;

    // TCP/IP Communication Manager codepoint constant.  Min. level 5.
    static final int CMNTCPIP = 0x1474;

    // Correlation Token codepoint constant.
    static final int CRRTKN = 0x2135;

    // Description Error code
    static final int DSCERRCD = 0x2101;

    // Server Attributes Reply Data codepoint constant.
    static final int EXCSATRD = 0x1443;

    // External Name codepoint constant.
    static final int EXTNAM = 0x115E;

    // Fixed Row Query Protocol.
    static final int FIXROWPRC = 0x2418;

    // Limited Block Query Protocol.
    static final int LMTBLKPRC = 0x2417;

    // Maximum Number of Extra Blocks.
    static final int MAXBLKEXT = 0x2141;

    // Manager Level List codepoint constant.
    static final int MGRLVLLS = 0x1404;

    // Manager Level Number Attribute constants.
    // Specifies the level of a defined DDM manager.
    static final int MGRLVLN = 0x1473;

    // Password
    static final int PASSWORD = 0x11A1;

    // Package name & consistency token
    static final int PKGNAMCT = 0x2112;

    // Conversational Protocol Error Code
    static final int PRCCNVCD = 0x113F;

    // Product Specific Identifier codepoint constant.
    static final int PRDID = 0x112E;

    // Product Specific Data
    static final int PRDDTA = 0x2104;

    // Query Attribute for Scrollability.
    static final int QRYATTSCR = 0x2149;

    // Query Attribute for Rowset
    static final int QRYATTSET = 0x214A;

    // Query Attribute for Sensitivity.
    static final int QRYATTSNS = 0x2157;

    // Query Attribute for Updatability.
    static final int QRYATTUPD = 0x2150;

    // Query Close Implicit
    static final int QRYCLSIMP = 0x215D;

    // Query Scroll Orientation.
    static final int QRYSCRORN = 0x2152;

    // Query Scroll Relative Orientation.
    static final int QRYSCRREL = 1;

    // Query Scroll Absolute Orientation.
    static final int QRYSCRABS = 2;

    // Query Scroll After Orientation.
    static final int QRYSCRAFT = 3;

    // Query Scroll Before Orientation.
    static final int QRYSCRBEF = 4;

    // Query Instance Identifier
    static final int QRYINSID = 0x215B;

    // Query Insensitive to Changes
    static final int QRYINS = 1;

    // Sensitive static
    static final int QRYSNSSTC = 0x2;

    // Query Attributes is Unknown or Undefined
    static final int QRYUNK = 0;

    // Query Row Number.
    static final int QRYROWNBR = 0x213D;

    // Query Block Reset.
    static final int QRYBLKRST = 0x2154;

    // Query Returns Data.
    static final int QRYRTNDTA = 0x2155;

    // Query Block Protocol Control
    static final int QRYBLKCTL = 0x2132;

    // Query Block Size
    static final int QRYBLKSZ = 0x2114;

    // Query Protocol Type
    static final int QRYPRCTYP = 0x2102;

    // Query Rowset Size.
    static final int QRYROWSET = 0x2156;

    // Cursor is Read-only.
    static final int QRYRDO = 0x1;

    // Cursor Allows Read, Delete, and Update Operations.
    static final int QRYUPD = 0x4;

    // Relational Database codepoint constant.  Min. level 3.
    static final int RDB = 0x240F;

    // RDB Access Manager Class.
    static final int RDBACCCL = 0x210F;

    // RDB Allow Updates
    static final int RDBALWUPD = 0x211A;

    // Relational Database Name codepoint constant.
    static final int RDBNAM = 0x2110;


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

    // Security Token codepoint constant.
    static final int SECTKN = 0x11DC;

    // SQL Application Manager codepoint constant.  Min. level 3.
    static final int SQLAM = 0x2407;

    // SQL Communication Area Reply Data codepoint constant.
    public static final int SQLCARD = 0x2408;

    // SQL Result Set Column Information Reply Data.
    public static final int SQLCINRD = 0x240B;

    // Hold Cursor Position
    static final int SQLCSRHLD = 0x211F;

    // SQL Result Set Reply Data.
    static final int SQLRSLRD = 0x240E;

    // SQLDA Reply Data codepoint constant.
    public static final int SQLDARD = 0x2411;

    // Server Class Name codepoint constant.
    static final int SRVCLSNM = 0x1147;


    // Server Name codepoint constant.
    static final int SRVNAM = 0x116D;

    // Server Product Release Level codepoint constant.
    static final int SRVRLSLV = 0x115A;

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
    public static final int TYPDEFNAM = 0x002F;

    // TYPDEF Overrides codepoint constant.
    public static final int TYPDEFOVR = 0x0035;

    // Unit of Work Disposition codepoint constant.
    static final int UOWDSP = 0x2115;

    // Unit of Work Disposition.  Committed Enumerated Value.
    static final int UOWDSP_COMMIT = 0x01;

    // Unit of Work Dispostion. Rolled Back Enumerated Value.
    static final int UOWDSP_ROLLBACK = 0x02;

    // Usrid codepoint constant.
    static final int USRID = 0x11A0;

    // Rdb Package Name, Consistency Token, and Section
    // Number codepoint constant.
    static final int PKGNAMCSN = 0x2113;

    // RDB Package Section Number
    static final int PKGSN = 0x210C;

    // Scalar Data Length
    static final int SCLDTALEN = 0x0100;

    // XA Manager
    static final int XAMGR = 0x1C01;

    // SQL Error Diagnostic Level
    static final int DIAGLVL = 0x2160;

    //-----------------------DDM reply codepoints---------------------------------

    // Command Check codepoint constant.
    public static final int CMDCHKRM = 0x1254;

    // Command Not Supported codepoint constant.
    static final int CMDNSPRM = 0x1250;

    // Abnormal End of Unit of Work Condition codepoint constant.
    static final int ABNUOWRM = 0x220D;

    // Access to RDB Completed.
    // Specifies that an instance of the SQL application manager
    // has been created and is bound to the specified RDB.
    static final int ACCRDBRM = 0x2201;


    final static int MGRLVLRM = 0x1210;

    // End Unit of Work Condition codepoint constant.
    static final int ENDUOWRM = 0x220C;

    // Object Not Supported codepoint constant.
    static final int OBJNSPRM = 0x1253;

    // Conversational Protocol Error
    public static final int PRCCNVRM = 0x1245;

    // Query not open codepoint constant.
    static final int QRYNOPRM = 0x2202;

    // Query previously opened codepoint
    static final int QRYPOPRM = 0x220F;

    // RDB Currently Accessed Codepoint
    static final int RDBACCRM = 0x2207;

    // RDB Commit Allowed codepoint
    static final int RDBCMTOK = 0x2105;

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

    // Data Stream Syntax Error
    public static final int SYNTAXRM = 0x124C;

    // Parameter Value Not Supported codepoint constant.
    public static final int VALNSPRM = 0x1252;

    // SQL Error Condition codepoint constant.
    static final int SQLERRRM = 0x2213;

    // Open Query Complete.
    public final static int OPNQRYRM = 0x2205;

    // End of Query.
    public final static int ENDQRYRM = 0x220B;

    // Data Descriptor Mismatch.
    final static int DTAMCHRM = 0x220E;

    // Open Query Failure.
    final static int OPNQFLRM = 0x2212;

    // RDB Result Set Reply Message.
    public final static int RSLSETRM = 0x2219;

    // Manager Level Overrides
    public static final int MGRLVLOVR = 0x1C03;

    //----------------------------fdoca code points-------------------------------

    static final int RTNEXTDTA = 0x2148;
    static final int RTNEXTROW = 0x01;
    static final int RTNEXTALL = 0x02;

    // Externalized FD:OCA Data codepoint constant.
    public static final int EXTDTA = 0x146C;

    // FDOCA data descriptor
    static final int FDODSC = 0x0010;

    // FDOCA data
    static final int FDODTA = 0x147A;

    //--------------------------ddm error code points---------------------------------
    // Syntax Error Code.  DSS header length less than 6.
    static int SYNERRCD_DSS_LESS_THAN_6 = 0x01;

    // Syntax Error Code.  DSS header length does not match the number of
    // bytes of data found.
    static int SYNERRCD_DSS_LENGTH_BYTE_NUMBER_MISMATCH = 0x02;

    // Syntax Error Code.  DSS header C-byte not D0.
    static int SYNERRCD_CBYTE_NOT_D0 = 0x03;

    // Syntax Error Code.  DSS header f-bytes either not recognized or not supported.
    static int SYNERRCD_FBYTE_NOT_SUPPORTED = 0x04;

    // Syntax Error Code.  Object length less than four.
    static int SYNERRCD_OBJ_LEN_LESS_THAN_4 = 0x07;

    // Syntax Error Code.  Object length not allowed.
    static int SYNERRCD_OBJ_LEN_NOT_ALLOWED = 0x0B;

    // Syntax Error Code.  Required object not found.
    static int SYNERRCD_REQ_OBJ_NOT_FOUND = 0x0E;

    // Syntax Error Code.  Duplicate object present.
    static int SYNERRCD_DUP_OBJ_PRESENT = 0x12;

    // Syntax Error Code.  Invalid request correlator specified.
    static int SYNERRCD_INVALID_CORRELATOR = 0x13;

    // Syntax Error Code.  Incorrect large object extended length field.
    static int SYNERRCD_INCORRECT_EXTENDED_LEN = 0x0C;

    // Syntax Error Code.  DSS continuation less than or equal to two.
    static int SYNERRCD_DSS_CONT_LESS_OR_EQUAL_2 = 0x16;

    // Syntax Error Code.  DSS chaining bit not b'1', but DSSFMT bit3 set to b'1'.
    static int SYNERRCD_CHAIN_OFF_SAME_NEXT_CORRELATOR = 0x18;

    // Syntax Error Code.  DSS chaining bit not b'1', but error continuation requested.
    static int SYNERRCD_CHAIN_OFF_ERROR_CONTINUE = 0x1A;

    // Conversational Protocol Error Code.  OBJDSS sent when not allowed.
    static int PRCCNVCD_OBJDSS_SENT_NOT_ALLOWED = 0x03;

    // Information Only Severity Code.
    static int SVRCOD_INFO = 0;

    // Warning Severity Code.
    static int SVRCOD_WARNING = 4;

    // Error Severity Code.
    static int SVRCOD_ERROR = 8;

    // Severe Error Severity Code.
    static int SVRCOD_SEVERE = 16;

    // Access Damage Severity Code.
    static int SVRCOD_ACCDMG = 32;

    // Permanent Damage Severity Code.
    static int SVRCOD_PRMDMG = 64;

    // Session Damage Severity Code.
    static int SVRCOD_SESDMG = 128;


    //--------------------------XA code points---------------------------

    // SYNC Point Control Reply
    public static final int SYNCCRD = 0x1248;

    // XA Return Value
    public static final int XARETVAL = 0x1904;

    // new unit of work for XA
    public static final int SYNCTYPE_NEW_UOW = 0x09;

    // End unit of work (Sync type).
    public static final int SYNCTYPE_END_UOW = 0x0B;

    // Prepare to commit (Sync type).
    public static final int SYNCTYPE_PREPARE = 0x01;

    // migrate to resync server sync type
    public static final int SYNCTYPE_MIGRATE = 0x02;

    // commit sync type
    public static final int SYNCTYPE_COMMITTED = 0x03;

    // request to forget sync type
    public static final int SYNCTYPE_REQ_FORGET = 0x06;

    //rollback sync type
    public static final int SYNCTYPE_ROLLBACK = 0x04;


    // migrated unit of work sync type
    public static final int SYNCTYPE_MIGRATED = 0x0A;

    //recover sync type
    public static final int SYNCTYPE_INDOUBT = 0x0C;

    // Length Codepoint
    public static final int LLCP = 0x0004;

    // SYNC Type Codepoint
    public static final int SYNCTYPE = 0x1187;

    // XId Codepoint
    public static final int XID = 0x1801;

    // XA Flag Codepoint
    public static final int XAFLAGS = 0x1903;


    // Resync Types
    public static final int RSYNC_FORGET = 0x02;

    // UOW States
    public static final int RESET_STATE = 0x01;
    public static final int UNKNOWN_STATE = 0x3;
    public static final int INDOUBT_STATE = 0x04;
    public static final int COLD_STATE = 0x05;

    // XA Flags
    public static final int TMNOFLAGS = 0x00000000;
    public static final int TMLOCAL = 0x10000000;

    // Prepared and hueristic complete list
    static final int PRPHRCLST = 0x1905;

    // XID count
    static final int XIDCNT = 0x1906;


    // hide the default constructor
    private CodePoint() {
    }
}


