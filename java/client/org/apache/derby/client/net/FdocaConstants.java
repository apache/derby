/*

   Derby - Class org.apache.derby.client.net.FdocaConstants

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

class FdocaConstants {

// PROTOCOL FD:OCA constants.
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
    static final int FDOCA_TYPE_FLOAT_X86 = 0x47;
    static final int FDOCA_TYPE_NFLOAT_X86 = 0xC7;
    static final int FDOCA_TYPE_FLOAT_IEEE = 0x48;
    static final int FDOCA_TYPE_NFLOAT_IEEE = 0xC8;
    static final int FDOCA_TYPE_LOBBYTES = 0x50;
    static final int FDOCA_TYPE_NLOBBYTES = 0xD0;
    static final int FDOCA_TYPE_LOBCHAR = 0x51;
    static final int FDOCA_TYPE_NLOBCHAR = 0xD1;

    // PROTOCOL Type constants.
    static final int PROTOCOL_TYPE_INTEGER = 0x02;
    static final int PROTOCOL_TYPE_NINTEGER = 0x03;
    static final int PROTOCOL_TYPE_SMALL = 0x04;
    static final int PROTOCOL_TYPE_NSMALL = 0x05;
    static final int PROTOCOL_TYPE_1BYTE_INT = 0x06;
    static final int PROTOCOL_TYPE_N1BYTE_INT = 0x07;
    static final int PROTOCOL_TYPE_FLOAT16 = 0x08;
    static final int PROTOCOL_TYPE_NFLOAT16 = 0x09;
    static final int PROTOCOL_TYPE_FLOAT8 = 0x0A;
    static final int PROTOCOL_TYPE_NFLOAT8 = 0x0B;
    static final int PROTOCOL_TYPE_FLOAT4 = 0x0C;
    static final int PROTOCOL_TYPE_NFLOAT4 = 0x0D;
    static final int PROTOCOL_TYPE_DECIMAL = 0x0E;
    static final int PROTOCOL_TYPE_NDECIMAL = 0x0F;
    static final int PROTOCOL_TYPE_NUMERIC_CHAR = 0x12;
    static final int PROTOCOL_TYPE_NNUMERIC_CHAR = 0x13;
    static final int PROTOCOL_TYPE_RSET_LOC = 0x14;
    static final int PROTOCOL_TYPE_NRSET_LOC = 0x15;
    static final int PROTOCOL_TYPE_INTEGER8 = 0x16;
    static final int PROTOCOL_TYPE_NINTEGER8 = 0x17;
    static final int PROTOCOL_TYPE_LOBLOC = 0x18;
    static final int PROTOCOL_TYPE_NLOBLOC = 0x19;
    static final int PROTOCOL_TYPE_CLOBLOC = 0x1A;
    static final int PROTOCOL_TYPE_NCLOBLOC = 0x1B;
    static final int PROTOCOL_TYPE_DBCSCLOBLOC = 0x1C;
    static final int PROTOCOL_TYPE_NDBCSCLOBLOC = 0x1D;
    static final int PROTOCOL_TYPE_DATE = 0x20;
    static final int PROTOCOL_TYPE_NDATE = 0x21;
    static final int PROTOCOL_TYPE_TIME = 0x22;
    static final int PROTOCOL_TYPE_NTIME = 0x23;
    static final int PROTOCOL_TYPE_TIMESTAMP = 0x24;
    static final int PROTOCOL_TYPE_NTIMESTAMP = 0x25;
    static final int PROTOCOL_TYPE_FIXBYTE = 0x26;
    static final int PROTOCOL_TYPE_NFIXBYTE = 0x27;
    static final int PROTOCOL_TYPE_VARBYTE = 0x28;
    static final int PROTOCOL_TYPE_NVARBYTE = 0x29;
    static final int PROTOCOL_TYPE_LONGVARBYTE = 0x2A;
    static final int PROTOCOL_TYPE_NLONGVARBYTE = 0x2B;
    static final int PROTOCOL_TYPE_NTERMBYTE = 0x2C;
    static final int PROTOCOL_TYPE_NNTERMBYTE = 0x2D;
    static final int PROTOCOL_TYPE_CSTR = 0x2E;
    static final int PROTOCOL_TYPE_NCSTR = 0x2F;
    static final int PROTOCOL_TYPE_CHAR = 0x30;
    static final int PROTOCOL_TYPE_NCHAR = 0x31;
    static final int PROTOCOL_TYPE_VARCHAR = 0x32;
    static final int PROTOCOL_TYPE_NVARCHAR = 0x33;
    static final int PROTOCOL_TYPE_LONG = 0x34;
    static final int PROTOCOL_TYPE_NLONG = 0x35;
    static final int PROTOCOL_TYPE_MIX = 0x3C;
    static final int PROTOCOL_TYPE_NMIX = 0x3D;
    static final int PROTOCOL_TYPE_VARMIX = 0x3E;
    static final int PROTOCOL_TYPE_NVARMIX = 0x3F;
    static final int PROTOCOL_TYPE_LONGMIX = 0x40;
    static final int PROTOCOL_TYPE_NLONGMIX = 0x41;
    static final int PROTOCOL_TYPE_CSTRMIX = 0x42;
    static final int PROTOCOL_TYPE_NCSTRMIX = 0x43;
    static final int PROTOCOL_TYPE_PSCLBYTE = 0x44;
    static final int PROTOCOL_TYPE_NPSCLBYTE = 0x45;

    // --- Override LIDs 0x50 - 0xAF
    static final int FIRST_OVERRIDE_LID = 0x50;
    static final int LAST_OVERRIDE_LID = 0xAF;

    static final int PROTOCOL_TYPE_LOBBYTES = 0xC8;
    static final int PROTOCOL_TYPE_NLOBBYTES = 0xC9;
    static final int PROTOCOL_TYPE_LOBCSBCS = 0xCA;
    static final int PROTOCOL_TYPE_NLOBCSBCS = 0xCB;
    static final int PROTOCOL_TYPE_LOBCDBCS = 0xCC;
    static final int PROTOCOL_TYPE_NLOBCDBCS = 0xCD;
    static final int PROTOCOL_TYPE_LOBCMIXED = 0xCE;
    static final int PROTOCOL_TYPE_NLOBCMIXED = 0xCF;

    static final int CPT_TRIPLET_TYPE = 0x7F;      // CPT triplet type
    static final int MDD_TRIPLET_TYPE = 0x78;      // MDD triplet type
    static final int NGDA_TRIPLET_TYPE = 0x76;     // N-GDA triplet type
    static final int RLO_TRIPLET_TYPE = 0x71;      // RLO triplet type
    static final int SDA_TRIPLET_TYPE = 0x70;      // SDA triplet type

    static final int SQLDTARD_LID = 0xF0;
    static final int SQLCADTA_LID = 0xE0;
    static final int SQLDTAGRP_LID = 0xD0;         // SQLDTAGRP LID
    static final int NULL_LID = 0x00;

    static final int INDICATOR_NULLABLE = 0x00;
    static final int NULL_DATA = 0xFF;

    static final int MAX_VARS_IN_NGDA = 84;        // Number of SQLVARs in full SQLDTAGRP
    // N-GDA or CPT
    static final int MDD_TRIPLET_SIZE = 7;         // Size of MDD triplet
    static final int SQLDTARD_RLO_SIZE = 6;        // size of SQLDTARD minus MDD
    static final int SQLCADTA_RLO_SIZE = 9;        // size of SQLCDTA minus MDD
    static final int SDA_TRIPLET_SIZE = 12;        // Size of SDA triplet
    static final int SQLDTAGRP_SIZE = 3;           // Size of SQLDTAGRP descriptor
    static final int CPT_SIZE = 3;                 // Size of CPT descriptor
    static final int SQLDTAGRP_COL_DSC_SIZE = 3;   // Env. LID & len. bytes
    static final int MDD_REST_SIZE = 5;            // Size of the MDD group minus length and type

    static final int MDD_SDA_TRIPLET_SIZE = 19;
    static final int MDD_SDA_PROTOCOLTYPE_OFFSET = 6;
    static final int MDD_SDA_FDOCALID_OFFSET = 9;
    static final int MDD_SDA_FDOCATYPE_OFFSET = 10;
    static final int MDD_SDA_CCSID_OFFSET = 11;
    static final int MDD_SDA_LENGTH_OFFSET = 17;

    static final byte[] defaultMddSdaBytes = {
        (byte) 0x07, (byte) 0x78, (byte) 0x00, (byte) 0x05,
        (byte) 0x01, (byte) 0x01, (byte) 0x00, // protocol type
        (byte) 0x0C, (byte) 0x70, (byte) 0x00, // fdoca triplet lid
        (byte) 0x00, // fdoca field type
        (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, // ccsid
        (byte) 0x01, (byte) 0x01,
        (byte) 0x7F, (byte) 0xFF                           // length
    };

    static final byte[] MDD_SQLDTAGRP_TOSEND = {
        (byte) 0x07, (byte) 0x78, (byte) 0x00, (byte) 0x05,
        (byte) 0x02, (byte) 0x01, (byte) 0xD0
    };

    static final byte[] MDD_SQLDTA_TOSEND = {
        (byte) 0x07, (byte) 0x78, (byte) 0x00, (byte) 0x05,
        (byte) 0x03, (byte) 0x01, (byte) 0xE4
    };

    // Hard-coded SQLDTA RLO
    static final byte[] SQLDTA_RLO_TOSEND = {
        (byte) 0x06, (byte) 0x71, (byte) 0xE4,
        (byte) 0xD0, (byte) 0x00, (byte) 0x01
    };

    // Hard-coded SQLCADTA RLO
    static final byte[] SQLCADTA_RLO_TOSEND = {
        (byte) 0x09, (byte) 0x71, (byte) 0xE0,
        (byte) 0x54, (byte) 0x00, (byte) 0x01,
        (byte) 0xD0, (byte) 0x00, (byte) 0x01
    };

    // Hard-coded SQLDTARD RLO
    static final byte[] SQLDTARD_RLO_TOSEND = {
        (byte) 0X06, (byte) 0X71, (byte) 0xF0,
        (byte) 0xE0, (byte) 0x00, (byte) 0x00
    };

    // following code added for parseSQLDTARD prototype
    static final int SQLDTARD_TRIPLET_TYPE_START = 0;
    static final int SQLDTARD_TRIPLET_TYPE_END = 1;
    static final int SQLDTARD_TRIPLET_TYPE_MDD = 2;
    static final int SQLDTARD_TRIPLET_TYPE_SDA = 3;
    static final int SQLDTARD_TRIPLET_TYPE_RLO = 4;
    static final int SQLDTARD_TRIPLET_TYPE_GDA = 5;
    static final int SQLDTARD_TRIPLET_TYPE_CPT = 6;

    static final boolean[][] SQLDTARD_TRIPLET_TYPES = {
        //   /*START*/, /*END*/, /*MDD*/, /*SDA*/, /*RLO*/, /*GDA*/, /*CPT*/    // next ->
/*START*/ {false, false, true, false, false, true, false},
/* END */ {false, false, false, false, false, false, false},
/* MDD */ {false, false, false, true, true, true, false}, //           |
/* SDA */ {false, false, true, false, false, false, false}, // previous  |
/* RLO */ {false, true, true, false, true, false, false}, //          \ /
/* GDA */ {false, false, true, false, true, false, true},
/* CPT */ {false, false, true, false, true, false, true}};

    static final int SQLDTARD_TRIPLET_ID_START = 0;
    static final int SQLDTARD_TRIPLET_ID_END = 1;
    static final int SQLDTARD_TRIPLET_ID_SDA = 2;
    static final int SQLDTARD_TRIPLET_ID_0 = 3;
    static final int SQLDTARD_TRIPLET_ID_D0 = 4;
    static final int SQLDTARD_TRIPLET_ID_E0 = 5;
    static final int SQLDTARD_TRIPLET_ID_F0 = 6;

    static final boolean[][] SQLDTARD_TRIPLET_IDS = {
        //   /*START*/, /*END*/, /*SDA*/, /*0*/, /*D0*/, /*E0*/, /*F4*/ // next ->
/*START*/  {false, false, false, true, true, false, false},
/* END */  {false, false, false, false, false, false, false},
/* SDA */  {false, false, false, true, false, false, false},
/* 0   */  {false, false, true, true, true, true, true},
/* D0 */   {false, false, false, true, false, true, false},
/* E0 */   {false, false, false, true, false, false, true},
/* F4 */   {false, true, false, false, false, false, false}};

    static final int RLO_GROUP_LID = 0;
    static final int RLO_ELEMENT_TAKEN = 1;
    static final int RLO_REP_FACTOR = 2;

    static final int[][] RLO_SQLCADTA = {// GROUP LID , ELEM TAKEN,  REP_FACTOR
        {0x54, 0, 1},
        {0xD0, 0, 1}};

    static final int[][] RLO_SQLDTARD = {// GROUP LID , ELEM TAKEN,  REP_FACTOR
        {0xE0, 0, 0}};

}
