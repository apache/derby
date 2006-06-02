/*

   Derby - Class org.apache.derby.client.net.NetConnectionRequest

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


import javax.transaction.xa.Xid;

import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.Utils;
import org.apache.derby.shared.common.reference.SQLState;

public class NetConnectionRequest extends Request implements ConnectionRequestInterface {
    NetConnectionRequest(NetAgent netAgent, CcsidManager ccsidManager, int bufferSize) {
        super(netAgent, ccsidManager, bufferSize);
    }
    //----------------------------- entry points ---------------------------------

    void writeExchangeServerAttributes(String externalName,
                                       int targetAgent,
                                       int targetSqlam,
                                       int targetRdb,
                                       int targetSecmgr,
                                       int targetCmntcpip,
                                       int targetCmnappc,
                                       int targetXamgr,
                                       int targetSyncptmgr,
                                       int targetRsyncmgr) throws SqlException {
        // send the exchange server attributes command to the server.
        // no other commands will be chained to the excsat because
        // the manager levels are needed before anything else is attempted.
        buildEXCSAT(externalName,
                targetAgent,
                targetSqlam,
                targetRdb,
                targetSecmgr,
                targetCmntcpip,
                targetCmnappc,
                targetXamgr,
                targetSyncptmgr,
                targetRsyncmgr);

    }

    void writeDummyExchangeServerAttributes() throws SqlException {
        // send the exchange server attributes command to the server,
        // without any parameters
        buildDummyEXCSAT();
    }

    void writeAccessSecurity(int securityMechanism,
                             String databaseName,
                             byte[] publicKey) throws SqlException {
        buildACCSEC(securityMechanism, databaseName, publicKey);
    }

    void writeSecurityCheck(int securityMechanism,
                            String databaseName,
                            String userid,
                            String password,
                            byte[] encryptedUserid,
                            byte[] encryptedPassword) throws SqlException {
        buildSECCHK(securityMechanism,
                databaseName,
                userid,
                password,
                encryptedUserid,
                encryptedPassword);
    }

    void writeAccessDatabase(String rdbnam,
                             boolean readOnly,
                             byte[] correlationToken,
                             byte[] productData,
                             Typdef typdef) throws SqlException {
        buildACCRDB(rdbnam,
                readOnly,
                correlationToken,
                productData,
                typdef);
    }


    public void writeCommitSubstitute(NetConnection connection) throws SqlException {
        buildDummyEXCSAT();
    }

    public void writeLocalCommit(NetConnection connection) throws SqlException {
        buildRDBCMM();
    }

    public void writeLocalRollback(NetConnection connection) throws SqlException {
        buildRDBRLLBCK();
    }

    public void writeLocalXAStart(NetConnection connection) throws SqlException {
    }


    //Build the SYNNCTL commit command
    public void writeLocalXACommit(NetConnection conn) throws SqlException {
    }

    //Build the SYNNCTL rollback command
    public void writeLocalXARollback(NetConnection conn) throws SqlException {
    }

    public void writeXaStartUnitOfWork(NetConnection conn) throws SqlException {
    }

    public void writeXaEndUnitOfWork(NetConnection conn) throws SqlException {
    }

    protected void writeXaPrepare(NetConnection conn) throws SqlException {
    }

    protected void writeXaCommit(NetConnection conn, Xid xid) throws SqlException {
    }

    protected void writeXaRollback(NetConnection conn, Xid xid) throws SqlException {
    }

    protected void writeXaRecover(NetConnection conn, int flag) throws SqlException {
    }

    protected void writeXaForget(NetConnection conn, Xid xid) throws SqlException {
    }

    public void writeSYNCType(int codepoint, int syncType) {
        writeScalar1Byte(codepoint, syncType);
    }

    public void writeForget(int codepoint, int value) {
    }

    public void writeReleaseConversation(int codepoint, int value) {
    }

    void writeNullXID(int codepoint) {
    }

    void writeXID(int codepoint, Xid xid) throws SqlException {
    }


    void writeXAFlags(int codepoint, int xaFlags) {
    }


    //----------------------helper methods----------------------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // RDB Commit Unit of Work (RDBCMM) Command commits all work performed
    // for the current unit of work.
    //
    // The Relational Database Name (RDBNAM) is an optional parameter
    // which will not be sent by this command to reduce size, building,
    // and parsing.
    void buildRDBCMM() throws SqlException {
        createCommand();
        writeLengthCodePoint(0x04, CodePoint.RDBCMM);
    }

    // RDB Rollback Unit of Work(RDBRLLBCK) Command rolls back
    // all work performed for the current unit of work.
    //
    // The Relational Database Name (RDBNAM) is an optional parameter
    // which will not be sent by this command to reduce size, building,
    // and parsing.
    void buildRDBRLLBCK() throws SqlException {
        createCommand();
        writeLengthCodePoint(0x04, CodePoint.RDBRLLBCK);
    }

    // build the Exchange Server Attributes Command.
    // This command sends the following information to the server.
    // - this driver's server class name
    // - this driver's level of each of the manager's it supports
    // - this driver's product release level
    // - this driver's external name
    // - this driver's server name
    void buildEXCSAT(String externalName,
                     int targetAgent,
                     int targetSqlam,
                     int targetRdb,
                     int targetSecmgr,
                     int targetCmntcpip,
                     int targetCmnappc,
                     int targetXamgr,
                     int targetSyncptmgr,
                     int targetRsyncmgr) throws SqlException {
        createCommand();

        // begin excsat collection by placing the 4 byte llcp in the buffer.
        // the length of this command will be computed later and "filled in"
        // with the call to request.updateLengthBytes().
        markLengthBytes(CodePoint.EXCSAT);

        // place the external name for the client into the buffer.
        // the external name was previously calculated before the call to this method.
        buildEXTNAM(externalName);

        // place the server name for the client into the buffer.
        buildSRVNAM("Derby");

        // place the server release level for the client into the buffer.
        // this is a hard coded value for the driver.
        buildSRVRLSLV();

        // the managers supported by this driver and their levels will
        // be sent to the server.  the variables which store these values
        // were initialized during object constrcution to the highest values
        // supported by the driver.

        // for the case of the manager levels object, there is no
        // need to have the length of the ddm object dynamically calculated
        // because this method knows exactly how many will be sent and can set
        // this now.
        // each manager level class and level are 4 bytes long and
        // right now 5 are being sent for a total of 20 bytes or 0x14 bytes.
        // writeScalarHeader will be called to insert the llcp.
        buildMGRLVLLS(targetAgent,
                targetSqlam,
                targetRdb,
                targetSecmgr,
                targetXamgr,
                targetSyncptmgr,
                targetRsyncmgr);


        // place the server class name into the buffer.
        // this value is hard coded for the driver.
        buildSRVCLSNM();

        // the excsat command is complete so the updateLengthBytes method
        // is called to dynamically compute the length for this command and insert
        // it into the buffer
        updateLengthBytes();
    }

    void buildDummyEXCSAT() throws SqlException {
        createCommand();

        // begin excsat collection by placing the 4 byte llcp in the buffer.
        // the length of this command will be computed later and "filled in"
        // with the call to request.updateLengthBytes().
        markLengthBytes(CodePoint.EXCSAT);

        // the excsat command is complete so the updateLengthBytes method
        // is called to dynamically compute the length for this command and insert
        // it into the buffer
        updateLengthBytes();
    }

    void buildACCSEC(int secmec,
                     String rdbnam,
                     byte[] sectkn) throws SqlException {
        createCommand();

        // place the llcp for the ACCSEC in the buffer.  save the length bytes for
        // later update
        markLengthBytes(CodePoint.ACCSEC);

        // the security mechanism is a required instance variable.  it will
        // always be sent.
        buildSECMEC(secmec);

        // the rdbnam will be built and sent.  different sqlam levels support
        // different lengths.  at this point the length has been checked against
        // the maximum allowable length.  so write the bytes and padd up to the
        // minimum length if needed.
        buildRDBNAM(rdbnam);

        if (sectkn != null) {
            buildSECTKN(sectkn);
        }

        // the accsec command is complete so notify the the request object to
        // update the ddm length and the dss header length.
        updateLengthBytes();
    }

    void buildSECCHK(int secmec,
                     String rdbnam,
                     String user,
                     String password,
                     byte[] sectkn,
                     byte[] sectkn2) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.SECCHK);

        // always send the negotiated security mechanism for the connection.
        buildSECMEC(secmec);

        // the rdbnam will be built and sent.  different sqlam levels support
        // different lengths.  at this point the length has been checked against
        // the maximum allowable length.  so write the bytes and padd up to the
        // minimum length if needed.
        buildRDBNAM(rdbnam);
        if (user != null) {
            buildUSRID(user);
        }
        if (password != null) {
            buildPASSWORD(password);
        }
        if (sectkn != null) {
            buildSECTKN(sectkn);
        }
        if (sectkn2 != null) {
            buildSECTKN(sectkn2);
        }
        updateLengthBytes();

    }

    // The Access RDB (ACCRDB) command makes a named relational database (RDB)
    // available to a requester by creating an instance of an SQL application
    // manager.  The access RDB command then binds the created instance to the target
    // agent and to the RDB. The RDB remains available (accessed) until
    // the communications conversation is terminate.
    void buildACCRDB(String rdbnam,
                     boolean readOnly,
                     byte[] crrtkn,
                     byte[] prddta,
                     Typdef typdef) throws SqlException {
        createCommand();

        markLengthBytes(CodePoint.ACCRDB);

        // the relational database name specifies the name of the rdb to
        // be accessed.  this can be different sizes depending on the level of
        // support.  the size will have ben previously checked so at this point just
        // write the data and pad with the correct number of bytes as needed.
        // this instance variable is always required.
        buildRDBNAM(rdbnam);

        // the rdb access manager class specifies an instance of the SQLAM
        // that accesses the RDB.  the sqlam manager class codepoint
        // is always used/required for this.  this instance variable
        // is always required.
        buildRDBACCCL();

        // product specific identifier specifies the product release level
        // of this driver.  see the hard coded value in the NetConfiguration class.
        // this instance variable is always required.
        buildPRDID();

        // product specific data.  this is an optional parameter which carries
        // product specific information.  although it is optional, it will be
        // sent to the server.  use the first byte to determine the number
        // of the prddta bytes to write to the buffer. note: this length
        // doesn't include itself so increment by it by 1 to get the actual
        // length of this data.
        buildPRDDTA(prddta);


        // the typdefnam parameter specifies the name of the data type to data representation
        // mappings used when this driver sends command data objects.
        buildTYPDEFNAM(typdef.getTypdefnam());

        if (crrtkn == null) {
            netAgent_.netConnection_.constructCrrtkn();
        }

        buildCRRTKN(netAgent_.netConnection_.crrtkn_);

        // This specifies the single-byte, double-byte
        // and mixed-byte CCSIDs of the Scalar Data Arrays (SDAs) in the identified
        // data type to the data representation mapping definitions.  This can
        // contain 3 CCSIDs.  The driver will only send the ones which were set.
        buildTYPDEFOVR(typdef.isCcsidSbcSet(),
                typdef.getCcsidSbc(),
                typdef.isCcsidDbcSet(),
                typdef.getCcsidDbc(),
                typdef.isCcsidMbcSet(),
                typdef.getCcsidMbc());

        // This specifies the SQL Error Diagnostic Level
        buildDIAGLVL();

        // RDB allow update is an optional parameter which indicates
        // whether the RDB allows the requester to perform update operations
        // in the RDB.  If update operations are not allowed, this connection
        // is limited to read-only access of the RDB resources.
        buildRDBALWUPD(readOnly);



        // the Statement Decimal Delimiter (STTDECDEL),
        // Statement String Delimiter (STTSTRDEL),
        // and Target Default Value Return (TRGDFTRT) are all optional
        // instance variables which will not be sent to the server.

        // the command and the dss are complete so make the call to notify
        // the request object.
        updateLengthBytes();
    }


    void buildSYNCCTLMigrate() throws SqlException {
    }

    void buildSYNCCTLCommit(int xaFlags, Xid xid) throws SqlException {
    }

    void buildSYNCCTLRollback(int xaFlags) throws SqlException {
    }


    // The External Name is the name of the job, task, or process on a
    // system for which a DDM server is active.
    private void buildEXTNAM(String extnam) throws SqlException {
        int extnamTruncateLength = Utils.min(extnam.length(),
                NetConfiguration.EXTNAM_MAXSIZE);

        writeScalarString(CodePoint.EXTNAM,
                extnam.substring(0, extnamTruncateLength));
    }

    // Server Name is the name of the DDM server.
    private void buildSRVNAM(String srvnam) throws SqlException {
        int srvnamTruncateLength = Utils.min(srvnam.length(),
                NetConfiguration.SRVNAM_MAXSIZE);
        writeScalarString(CodePoint.SRVNAM,
                srvnam.substring(0, srvnamTruncateLength));
    }

    // Server Product Release Level String specifies the product
    // release level of a DDM server.
    private void buildSRVRLSLV() throws SqlException {
        // Hard-coded to ClientDNC 1.0 for dnc 1.0.
        writeScalarString(CodePoint.SRVRLSLV, NetConfiguration.SRVRLSLV);
    }

    private void buildSRVCLSNM() throws SqlException {
        // Server class name is hard-coded to QDERBY/JVM for dnc.
        writeScalarString(CodePoint.SRVCLSNM, NetConfiguration.SRVCLSNM_JVM);
    }

    // Precondition: valid secmec is assumed.
    private void buildSECMEC(int secmec) throws SqlException {
        writeScalar2Bytes(CodePoint.SECMEC, secmec);
    }

    // Relational Database Name specifies the name of a relational database
    // of the server.
    // if length of RDB name <= 18 characters, there is not change to the format
    // of the RDB name.  The length of the RDBNAM remains fixed at 18 which includes
    // any right bland padding if necessary.
    // if length of the RDB name is > 18 characters, the length of the RDB name is
    // identical to the length of the RDB name.  No right blank padding is required.
    private void buildRDBNAM(String rdbnam) throws SqlException {
        // since this gets built more than once on the connect flow,
        // see if we can optimize

        int rdbnamLength = rdbnam.length();
        if (rdbnamLength <= NetConfiguration.PKG_IDENTIFIER_FIXED_LEN) {
            writeScalarPaddedString(CodePoint.RDBNAM,
                    rdbnam,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);  // minimum length of RDBNAM
        } else {
            if (rdbnamLength <= NetConfiguration.PKG_IDENTIFIER_MAX_LEN) {
                writeScalarString(CodePoint.RDBNAM, rdbnam);
            } else {
                throw new SqlException(netAgent_.logWriter_, 
                    new ClientMessageId(SQLState.NET_DBNAME_TOO_LONG), rdbnam);
            }
            //"at SQLAM level " + netAgent_.targetSqlam_);
        }
    }

    private void buildSECTKN(byte[] sectkn) throws SqlException {
        if (sectkn.length > NetConfiguration.SECTKN_MAXSIZE) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_SECTKN_TOO_LONG));
        }
        writeScalarBytes(CodePoint.SECTKN, sectkn);
    }

    private void buildUSRID(String usrid) throws SqlException {
        int usridLength = usrid.length();
        if ((usridLength == 0) || (usridLength > NetConfiguration.USRID_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_USERID_TOO_LONG));
        }

        writeScalarString(CodePoint.USRID, usrid);
    }

    private void buildPASSWORD(String password) throws SqlException {
        int passwordLength = password.length();
        if ((passwordLength == 0) || (passwordLength > NetConfiguration.PASSWORD_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_PASSWORD_TOO_LONG));
        }
        if (netAgent_.logWriter_ != null) {
            // remember the position of password in order to
            // mask it out in trace (see Request.sendBytes()).
            passwordIncluded_ = true;
            passwordStart_ = offset_ + 4;
            passwordLength_ = passwordLength;
        }
        writeScalarString(CodePoint.PASSWORD, password);
    }

    private void buildRDBACCCL() throws SqlException {
        writeScalar2Bytes(CodePoint.RDBACCCL, CodePoint.SQLAM);
    }


    private void buildPRDID() throws SqlException {
        writeScalarString(CodePoint.PRDID, NetConfiguration.PRDID);  // product id is hard-coded to DNC01000 for dnc 1.0.
    }

    private void buildPRDDTA(byte[] prddta) throws SqlException {
        int prddtaLength = (prddta[NetConfiguration.PRDDTA_LEN_BYTE] & 0xff) + 1;
        writeScalarBytes(CodePoint.PRDDTA, prddta, 0, prddtaLength);
    }

    private void buildTYPDEFNAM(String typdefnam) throws SqlException {
        writeScalarString(CodePoint.TYPDEFNAM, typdefnam);
    }

    void buildTYPDEFOVR(boolean sendCcsidSbc,
                        int ccsidSbc,
                        boolean sendCcsidDbc,
                        int ccsidDbc,
                        boolean sendCcsidMbc,
                        int ccsidMbc) throws SqlException {
        markLengthBytes(CodePoint.TYPDEFOVR);
        // write the single-byte ccsid used by this driver.
        if (sendCcsidSbc) {
            writeScalar2Bytes(CodePoint.CCSIDSBC, ccsidSbc);
        }

        // write the double-byte ccsid used by this driver.
        if (sendCcsidDbc) {
            writeScalar2Bytes(CodePoint.CCSIDDBC, ccsidDbc);
        }

        // write the mixed-byte ccsid used by this driver
        if (sendCcsidMbc) {
            writeScalar2Bytes(CodePoint.CCSIDMBC, ccsidMbc);
        }

        updateLengthBytes();

    }

    private void buildDIAGLVL() throws SqlException {
        markLengthBytes(CodePoint.DIAGLVL);
        
        writeByte(CodePoint.DIAGLVL2);
        updateLengthBytes();
    }

    private void buildMGRLVLLS(int agent,
                               int sqlam,
                               int rdb,
                               int secmgr,
                               int xamgr,
                               int syncptmgr,
                               int rsyncmgr) throws SqlException {
        markLengthBytes(CodePoint.MGRLVLLS);

        // place the managers and their levels in the buffer
        writeCodePoint4Bytes(CodePoint.AGENT, agent);
        writeCodePoint4Bytes(CodePoint.SQLAM, sqlam);
        writeCodePoint4Bytes(CodePoint.RDB, rdb);
        writeCodePoint4Bytes(CodePoint.SECMGR, secmgr);

        if (netAgent_.netConnection_.isXAConnection()) {
            if (xamgr != NetConfiguration.MGRLVL_NA) {
                writeCodePoint4Bytes(CodePoint.XAMGR, xamgr);
            }
            if (syncptmgr != NetConfiguration.MGRLVL_NA) {
                writeCodePoint4Bytes(CodePoint.SYNCPTMGR, syncptmgr);
            }
            if (rsyncmgr != NetConfiguration.MGRLVL_NA) {
                writeCodePoint4Bytes(CodePoint.RSYNCMGR, rsyncmgr);
            }
        }
        updateLengthBytes();
    }

    private void buildCRRTKN(byte[] crrtkn) throws SqlException {
        writeScalarBytes(CodePoint.CRRTKN, crrtkn);
    }

    private void buildRDBALWUPD(boolean readOnly) throws SqlException {
        if (readOnly) {
            writeScalar1Byte(CodePoint.RDBALWUPD, CodePoint.FALSE);
        }
    }

}



