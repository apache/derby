/*

   Derby - Class org.apache.derby.client.net.NetConfiguration

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

import org.apache.derby.iapi.reference.DRDAConstants;
import org.apache.derby.client.am.Version;

public class NetConfiguration {
    // ---------------------------------------------------------------------------

    static final int INITIAL_CAPACITY = 10; // initial size of the Vector for the input parameters

    // Value to use when padding non-character data in ddm objects.
    public static final byte NON_CHAR_DDM_DATA_PAD_BYTE = 0x00;

    static final int CURSOR_NOT_OPEN = 0;
    static final int CURSOR_OPEN = 1;
    static final int CURSOR_CLOSE_ONLY = 2;

    // Number of bytes in the DDM header.
    static final int DDMHDR_LENGTH = 4;

    // Number of bytes in the DSS header.
    static final int DSSHDR_LENGTH = 6;

    // Maximum size of External Name.
    static final int EXTNAM_MAXSIZE = 255;

    // Minimum agent level required by protocol.
    static final int MIN_AGENT_MGRLVL = 3;

    // Minimum communications tcpip manager level required by protocol.
    static final int MIN_CMNTCPIP_MGRLVL = 5;

    // Minimum LU6.2 Conversational Communications Manager
    static final int MIN_CMNAPPC_MGRLVL = 3;

    // Minimum rdb manager level required by protocol.
    static final int MIN_RDB_MGRLVL = 3;

    // Minimum secmgr manager level required by protocol.
    static final int MIN_SECMGR_MGRLVL = 5;

    // Minimum sqlam manager level required by protocol.
    static final int MIN_SQLAM_MGRLVL = 4;

    // Minimum xa manager level required by protocol.
    static final int MIN_XAMGR_MGRLVL = 7;

    // Minimum secmgr manager level required by protocol.
    static final int MIN_SYNCPTMGR_MGRLVL = 5;

    // Minimum sqlam manager level required by protocol.
    static final int MIN_RSYNCMGR_MGRLVL = 5;

    // Maximun Password size.
    static final int PASSWORD_MAXSIZE = 255;

    // Fixed PRDDTA application id fixed length.
    public static final int PRDDTA_APPL_ID_FIXED_LEN = 20;

    // PRDDTA Accounting Suffix Length byte offset.
    static final int PRDDTA_ACCT_SUFFIX_LEN_BYTE = 55;

    // PRDDTA Max Accounting Suffix Length.
    static final int PRDDTA_MAX_ACCT_SUFFIX_LEN = 199;

    // PRDDTA Length byte offset.
    static final int PRDDTA_LEN_BYTE = 0;

    // Maximum PRDDTA size.
    static final int PRDDTA_MAXSIZE = 255;

    // PRDDTA platform id.
    public static final String PRDDTA_PLATFORM_ID = "JVM               ";

    // Fixed PRDDTA user id fixed length.
    public static final int PRDDTA_USER_ID_FIXED_LEN = 8;

    // Identifier Length for fixed length rdb name
    public static final int PKG_IDENTIFIER_FIXED_LEN = 18;

    // Maximum RDB Identifier Length
    static final int PKG_IDENTIFIER_MAX_LEN = 255;

    // Fixed pkgcnstkn length
    public static final int PKGCNSTKN_FIXED_LEN = 8;

    // Maximum length of a security token.
    // Anything greater than 32763 bytes of SECTKN would require extended length DDMs.
    // This seems like an impossible upper bound limit right now so set
    // max to 32763 and cross bridge later.
    static final int SECTKN_MAXSIZE = 32763;  // this was 255

    // Server class name of the ClientDNC product.
    static final String SRVCLSNM_JVM = "QDERBY/JVM";

    // Maximum size of SRVNAM Name.
    static final int SRVNAM_MAXSIZE = 255;

    // Manager is NA or not usued.
    static final int MGRLVL_NA = 0;

    // Manager Level 3 constant.
    static final int MGRLVL_3 = 0x03;

    // Manager Level 4 constant.
    static final int MGRLVL_4 = 0x04;

    // Manager Level 5 constant.
    static final int MGRLVL_5 = 0x05;

    // Manager level 6 constant.
    static final int MGRLVL_6 = 0x06;

    // Manager Level 7 constant.
    static final public int MGRLVL_7 = 0x07;

    // Indicates the DCE security mechanism.
    static final int SECMEC_DCESEC = 0x01;

    // Indicates encrypted userid, password,
    // new password security mechanism.
    static final int SECMEC_EUSRIDNWPWD = 0x0A;

    // Indicates userid/encrypted password security mechanism.
    public static final int SECMEC_EUSRIDPWD = 0x09;

    // Indicates userid only security mechanism.
    public static final int SECMEC_USRIDONL = 0x04;

    // Indicates userid/encrypted password security mechanism.
    public static final int SECMEC_USRENCPWD = 0x07;

    // Indicates userid/new password security mechanism.
    static final int SECMEC_USRIDNWPWD = 0x05;

    // Indicates userid/password security mechanism.
    public static final int SECMEC_USRIDPWD = 0x03;


    //Indicates Encrypted userid and Encrypted Security-sensitive Data security mechanism
    public static final int SECMEC_EUSRIDDTA = 0x0C;

    //Indicates Encrypted userid,Encrypted password and Encrypted Security-sensitive Data security mechanism
    public static final int SECMEC_EUSRPWDDTA = 0x0D;

    // list of security mechanisms supported by this driver
    static final int[] SECMGR_SECMECS = {NetConfiguration.SECMEC_EUSRIDPWD,
                                         NetConfiguration.SECMEC_USRENCPWD,
                                         NetConfiguration.SECMEC_USRIDPWD,
                                         NetConfiguration.SECMEC_USRIDONL,
                                         NetConfiguration.SECMEC_EUSRIDDTA,
                                         NetConfiguration.SECMEC_EUSRPWDDTA};


    // IEEE ASCII constant.
    public static final String SYSTEM_ASC = "QTDSQLASC";

    // JVM constant.
    static final String SYSTEM_JVM = "QTDSQLJVM";

    // Maximum size of User Name.
    static final int USRID_MAXSIZE = 255;

    // Product id of the ClientDNC.
    public static String PRDID;

    // The server release level of this product.
    // It will be prefixed with PRDID
    static String SRVRLSLV;

    // Initialize PRDID and SRVRLSLV
    static {
        int majorVersion = Version.getMajorVersion();
        int minorVersion = Version.getMinorVersion();
        int protocolMaintVersion = Version.getProtocolMaintVersion();

        // PRDID format as Network Server expects  it: DNCMMmx
        // MM = major version
        // mm = minor version
        // x = protocol MaintenanceVersion

        String prdId = DRDAConstants.DERBY_DRDA_CLIENT_ID;
        if (majorVersion < 10) {
            prdId += "0";
        }
        prdId += majorVersion;

        if (minorVersion < 10) {
            prdId += "0";
        }

        prdId += minorVersion;
        prdId += protocolMaintVersion;
        PRDID = prdId;
        SRVRLSLV = prdId + "/" + Version.getDriverVersion();
    }

}
