/*

   Derby - Class org.apache.derby.client.net.NetPackageRequest

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
package org.apache.derby.client.net;

import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.SQLState;


class NetPackageRequest extends NetConnectionRequest {
    private static final String COLLECTIONNAME = "NULLID";

    NetPackageRequest(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    private void buildCommonPKGNAMinfo(Section section) throws SqlException {
        String collectionToFlow = COLLECTIONNAME;
        // the scalar data length field may or may not be required.  it depends
        // on the level of support and length of the data.
        // check the lengths of the RDBNAM, RDBCOLID, and PKGID.
        // Determine if the lengths require an SCLDTALEN object.
        // Note: if an SQLDTALEN is required for ONE of them,
        // it is needed for ALL of them.  This is why this check is
        // up front.
        // the SQLAM level dictates the maximum size for
        // RDB Collection Identifier (RDBCOLID)
        // Relational Database Name (RDBNAM)
        // RDB Package Identifier (PKGID)
        int maxIdentifierLength = NetConfiguration.PKG_IDENTIFIER_MAX_LEN;
        CcsidManager ccsidMgr = netAgent_.getCurrentCcsidManager();

        byte[] dbnameBytes = ccsidMgr.convertFromJavaString(
                netAgent_.netConnection_.databaseName_, netAgent_);

        byte[] collectionToFlowBytes = ccsidMgr.convertFromJavaString(
                collectionToFlow, netAgent_);

        byte[] pkgNameBytes = ccsidMgr.convertFromJavaString(
                section.getPackageName(), netAgent_);

        //Maximum RDBNAM length would depend on the server level. 
        // Server under 10.11 support only 255 bytes but server
        // at 10.11 and higher support higher limit of 1024
        boolean scldtalenRequired =
                checkPKGNAMlengths(netAgent_.netConnection_.databaseName_,
                dbnameBytes.length,
                (netAgent_.netConnection_.databaseMetaData_.serverSupportLongRDBNAM())? 
                        NetConfiguration.RDBNAM_MAX_LEN 
                        : NetConfiguration.PKG_IDENTIFIER_MAX_LEN,  
                NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(collectionToFlow,
                    collectionToFlowBytes.length,
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(section.getPackageName(),
                    pkgNameBytes.length,
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        // the format is different depending on if an SCLDTALEN is required.
        if (!scldtalenRequired) {
            byte padByte = ccsidMgr.space_;
            writeScalarPaddedBytes(dbnameBytes,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN, padByte);
            writeScalarPaddedBytes(collectionToFlowBytes,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN, padByte);
            writeScalarPaddedBytes(pkgNameBytes,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN, padByte);
        } else {
            buildSCLDTA(dbnameBytes, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(collectionToFlowBytes, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(pkgNameBytes, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }
    }

    private void buildSCLDTA(byte[] identifier, int minimumLength)
            throws SqlException {
        int length = Math.max(minimumLength, identifier.length);
        write2Bytes(length);
        byte padByte = netAgent_.getCurrentCcsidManager().space_;
        writeScalarPaddedBytes(identifier, length, padByte);
    }


    // this specifies the fully qualified package name,
    // consistency token, and section number within the package being used
    // to execute the SQL.  If the connection supports reusing the previous
    // package information and this information is the same except for the section
    // number then only the section number needs to be sent to the server.
    void buildPKGNAMCSN(Section section) throws SqlException {
        if (!canCommandUseDefaultPKGNAMCSN()) {
            markLengthBytes(CodePoint.PKGNAMCSN);
            // If PKGNAMCBytes is already available, copy the bytes to the request buffer directly.
            if (section.getPKGNAMCBytes() != null) {
                writeStoredPKGNAMCBytes(section);
            } else {
                // Mark the beginning of PKGNAMCSN bytes.
                markForCachingPKGNAMCSN();
                buildCommonPKGNAMinfo(section);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                writeScalarPaddedBytes(
                        Configuration.getDncPackageConsistencyToken(),
                        NetConfiguration.PKGCNSTKN_FIXED_LEN,
                        NetConfiguration.NON_CHAR_DDM_DATA_PAD_BYTE);
                // store the PKGNAMCbytes
                storePKGNAMCBytes(section);
            }
            write2Bytes(section.getSectionNumber());
            updateLengthBytes();
        } else {
            writeScalar2Bytes(CodePoint.PKGSN, section.getSectionNumber());
        }
    }

    private void storePKGNAMCBytes(Section section) {
        // Get the locaton where we started writing PKGNAMCSN
        int startPos = popMarkForCachingPKGNAMCSN();
//IC see: https://issues.apache.org/jira/browse/DERBY-5210
        byte[] b = new byte[buffer.position() - startPos];
        buffer.position(startPos);
        buffer.get(b);
        section.setPKGNAMCBytes(b);
    }

    private void writeStoredPKGNAMCBytes(Section section) {
        writeBytes(section.getPKGNAMCBytes());
    }

    private boolean canCommandUseDefaultPKGNAMCSN() {
        return false;
    }


    // throws an exception if lengths exceed the maximum.
    // returns a boolean indicating if SLCDTALEN is required.
    private boolean checkPKGNAMlengths(String identifier,
                                       int length,
                                       int maxIdentifierLength,
                                       int lengthRequiringScldta) throws SqlException {
        if (length > maxIdentifierLength) {
            throw new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.LANG_IDENTIFIER_TOO_LONG),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                identifier, maxIdentifierLength);
        }

        return (length > lengthRequiringScldta);
    }

    private void buildNOCMorNOCS(String string) throws SqlException {
        if (string == null) {
            write2Bytes(0xffff);
        } else {
            if (netAgent_.typdef_.isCcsidMbcSet()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                byte[] sqlBytes =
                    string.getBytes(netAgent_.typdef_.getCcsidMbcEncoding());
                write1Byte(0x00);
                write4Bytes(sqlBytes.length);
                writeBytes(sqlBytes, sqlBytes.length);
                write1Byte(0xff);
            } else {
                byte[] sqlBytes =
                    string.getBytes(netAgent_.typdef_.getCcsidSbcEncoding());
                write1Byte(0xff);
                write1Byte(0x00);
                write4Bytes(sqlBytes.length);
                writeBytes(sqlBytes, sqlBytes.length);
            }
        }
    }

    // SQLSTTGRP : FDOCA EARLY GROUP
    // SQL Statement Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLSTATEMENT_m; PROTOCOL TYPE LVCM; ENVLID 0x40; Length Override 32767
    //   SQLSTATEMENT_s; PROTOCOL TYPE LVCS; ENVLID 0x34; Length Override 32767
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLSTATEMENT_m; PROTOCOL TYPE NOCM; ENVLID 0xCF; Length Override 4
    //   SQLSTATEMENT_s; PROTOCOL TYPE NOCS; ENVLID 0xCB; Length Override 4
    private void buildSQLSTTGRP(String string) throws SqlException {
        buildNOCMorNOCS(string);
    }

    // SQLSTT : FDOCA EARLY ROW
    // SQL Statement Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLSTTGRP; GROUP LID 0x5C; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void buildSQLSTT(String string) throws SqlException {
        buildSQLSTTGRP(string);
    }

    protected void buildSQLSTTcommandData(String sql) throws SqlException {
        createEncryptedCommandData();
        int loc = buffer.position();
        markLengthBytes(CodePoint.SQLSTT);
        buildSQLSTT(sql);
        updateLengthBytes();
        if (netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRPWDDTA) {
            encryptDataStream(loc);
        }

    }


    protected void buildSQLATTRcommandData(String sql) throws SqlException {
        createEncryptedCommandData();
        int loc = buffer.position();
        markLengthBytes(CodePoint.SQLATTR);
        buildSQLSTT(sql);
        updateLengthBytes();
        if (netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRPWDDTA) {
            encryptDataStream(loc);
        }

    }


//IC see: https://issues.apache.org/jira/browse/DERBY-6125
    void encryptDataStream(int lengthLocation) throws SqlException {
//IC see: https://issues.apache.org/jira/browse/DERBY-5210
        byte[] clearedBytes = new byte[buffer.position() - lengthLocation];
        buffer.position(lengthLocation);
        buffer.get(clearedBytes);

        byte[] encryptedBytes;

        encryptedBytes = netAgent_.netConnection_.getEncryptionManager().
                encryptData(clearedBytes,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        netAgent_.netConnection_.getTargetPublicKey(),
                        netAgent_.netConnection_.getTargetPublicKey());

//IC see: https://issues.apache.org/jira/browse/DERBY-5210
        buffer.position(lengthLocation);
        writeBytes(encryptedBytes);

        //we need to update the length in DSS header here.
        buffer.putShort(lengthLocation - 6, (short) encryptedBytes.length);
    }

}
