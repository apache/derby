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


public class NetPackageRequest extends NetConnectionRequest {
    static final String COLLECTIONNAME = "NULLID";

    NetPackageRequest(NetAgent netAgent, CcsidManager ccsidManager, int bufferSize) {
        super(netAgent, ccsidManager, bufferSize);
    }

    // RDB Package Name, Consistency Token
    // Scalar Object specifies the fully qualified name of a relational
    // database package and its consistency token.
    //
    // To accomodate larger lengths, the Scalar Data Length
    // (SCLDTALEN) Field is used to specify the length of the instance
    // variable which follows.
    static final String collectionName = "NULLID";

    void buildCommonPKGNAMinfo(Section section) throws SqlException {
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

        boolean scldtalenRequired = false;
        scldtalenRequired = checkPKGNAMlengths(netAgent_.netConnection_.databaseName_,
                maxIdentifierLength,
                NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(collectionToFlow,
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(section.getPackageName(),
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        // the format is different depending on if an SCLDTALEN is required.
        if (!scldtalenRequired) {
            writeScalarPaddedString(netAgent_.netConnection_.databaseName_,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            writeScalarPaddedString(collectionToFlow,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            writeScalarPaddedString(section.getPackageName(),
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        } else {
            buildSCLDTA(netAgent_.netConnection_.databaseName_, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(collectionToFlow, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(section.getPackageName(), NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }
    }

    private void buildSCLDTA(String identifier, int minimumLength) throws SqlException {
        if (identifier.length() <= minimumLength) {
            write2Bytes(minimumLength);
            writeScalarPaddedString(identifier, minimumLength);
        } else {
            write2Bytes(identifier.length());
            writeScalarPaddedString(identifier, identifier.length());
        }
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
                writeScalarPaddedBytes(Configuration.dncPackageConsistencyToken,
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
        int copyLength = offset_ - startPos;
        byte[] b = new byte[copyLength];
        System.arraycopy(bytes_,
                startPos,
                b,
                0,
                copyLength);
        section.setPKGNAMCBytes(b);
    }

    private void writeStoredPKGNAMCBytes(Section section) {
        byte[] b = section.getPKGNAMCBytes();

        // Mare sure request buffer has enough space to write this byte array.
        ensureLength(offset_ + b.length);

        System.arraycopy(b,
                0,
                bytes_,
                offset_,
                b.length);

        offset_ += b.length;
    }

    private boolean canCommandUseDefaultPKGNAMCSN() {
        return false;
    }


    // throws an exception if lengths exceed the maximum.
    // returns a boolean indicating if SLCDTALEN is required.
    private boolean checkPKGNAMlengths(String identifier,
                                       int maxIdentifierLength,
                                       int lengthRequiringScldta) throws SqlException {
        int length = identifier.length();
        if (length > maxIdentifierLength) {
            throw new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.LANG_IDENTIFIER_TOO_LONG),
                identifier, new Integer(maxIdentifierLength));
        }

        return (length > lengthRequiringScldta);
    }

    private byte[] getBytes(String string, String encoding) throws SqlException {
        try {
            return string.getBytes(encoding);
        } catch (java.lang.Exception e) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.JAVA_EXCEPTION), 
                e.getClass().getName(), e.getMessage(), e);
        }
    }

    private void buildNOCMorNOCS(String string) throws SqlException {
        if (string == null) {
            write2Bytes(0xffff);
        } else {
            byte[] sqlBytes = null;

            if (netAgent_.typdef_.isCcsidMbcSet()) {
                sqlBytes = getBytes(string, netAgent_.typdef_.getCcsidMbcEncoding());
                write1Byte(0x00);
                write4Bytes(sqlBytes.length);
                writeBytes(sqlBytes, sqlBytes.length);
                write1Byte(0xff);
            } else {
                sqlBytes = getBytes(string, netAgent_.typdef_.getCcsidSbcEncoding());
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
        return;
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
        int loc = offset_;
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
        int loc = offset_;
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


    public void encryptDataStream(int lengthLocation) throws SqlException {
        byte[] clearedBytes = new byte[offset_ - lengthLocation];
        byte[] encryptedBytes;
        for (int i = lengthLocation; i < offset_; i++) {
            clearedBytes[i - lengthLocation] = bytes_[i];
        }

        encryptedBytes = netAgent_.netConnection_.getEncryptionManager().
                encryptData(clearedBytes,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        netAgent_.netConnection_.getTargetPublicKey(),
                        netAgent_.netConnection_.getTargetPublicKey());

        int length = encryptedBytes.length;

        if (bytes_.length >= lengthLocation + length) {
            System.arraycopy(encryptedBytes, 0, bytes_, lengthLocation, length);
        } else {
            byte[] largeByte = new byte[lengthLocation + length];
            System.arraycopy(bytes_, 0, largeByte, 0, lengthLocation);
            System.arraycopy(encryptedBytes, 0, largeByte, lengthLocation, length);
            bytes_ = largeByte;
        }

        offset_ += length - clearedBytes.length;

        //we need to update the length in DSS header here.

        bytes_[lengthLocation - 6] = (byte) ((length >>> 8) & 0xff);
        bytes_[lengthLocation - 5] = (byte) (length & 0xff);
    }

}
