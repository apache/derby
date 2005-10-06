/*

   Derby - Class org.apache.derby.client.net.NetConnectionReply

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

import org.apache.derby.client.am.Connection;
import org.apache.derby.client.am.ConnectionCallbackInterface;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.SqlState;
import org.apache.derby.client.am.Sqlca;

public class NetConnectionReply extends Reply
        implements ConnectionReplyInterface {
    NetConnectionReply(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    // NET only entry point
    void readExchangeServerAttributes(Connection connection) throws SqlException {
        startSameIdChainParse();
        parseEXCSATreply((NetConnection) connection);
        endOfSameIdChainData();
        agent_.checkForChainBreakingException_();
    }

    void verifyDeferredReset() throws SqlException {
        readDssHeader();
        verifyConnectReply(CodePoint.EXCSATRD);
        readDssHeader();
        verifyConnectReply(CodePoint.ACCSECRD);
        readDssHeader();
        verifyConnectReply(CodePoint.SECCHKRM);
        readDssHeader();
        verifyConnectReply(CodePoint.ACCRDBRM);
        agent_.checkForChainBreakingException_();
    }

    void verifyConnectReply(int codept) throws SqlException {
        if (peekCodePoint() != codept) {
            parseConnectError();
            return;
        }
        readLengthAndCodePoint();
        skipBytes();

        if (codept == CodePoint.ACCRDBRM) {
            int peekCP = peekCodePoint();
            if (peekCP == Reply.END_OF_SAME_ID_CHAIN) {
                return;
            }

            parseTypdefsOrMgrlvlovrs();
            NetSqlca netSqlca = parseSQLCARD(null);
            netAgent_.netConnection_.completeSqlca(netSqlca);
        }
    }

    void parseConnectError() throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.MGRLVLRM:
            parseMGRLVLRM();
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void readDummyExchangeServerAttributes(Connection connection) throws SqlException {
        startSameIdChainParse();
        parseDummyEXCSATreply((NetConnection) connection);
        endOfSameIdChainData();
        agent_.checkForChainBreakingException_();
    }

    // NET only entry point
    void readAccessSecurity(Connection connection,
                            int securityMechanism) throws SqlException {
        startSameIdChainParse();
        parseACCSECreply((NetConnection) connection, securityMechanism);
        endOfSameIdChainData();
        agent_.checkForChainBreakingException_();
    }

    // NET only entry point
    void readSecurityCheck(Connection connection) throws SqlException {
        startSameIdChainParse();
        parseSECCHKreply((NetConnection) connection);
        endOfSameIdChainData();
        agent_.checkForChainBreakingException_();
    }

    // NET only entry point
    void readAccessDatabase(Connection connection) throws SqlException {
        startSameIdChainParse();
        parseACCRDBreply((NetConnection) connection);
        endOfSameIdChainData();
        agent_.checkForChainBreakingException_();
    }


    public void readCommitSubstitute(ConnectionCallbackInterface connection) throws DisconnectException {
        startSameIdChainParse();
        parseDummyEXCSATreply((NetConnection) connection);
        endOfSameIdChainData();
    }

    public void readLocalCommit(ConnectionCallbackInterface connection) throws DisconnectException {
        startSameIdChainParse();
        parseRDBCMMreply(connection);
        endOfSameIdChainData();
    }

    public void readLocalRollback(ConnectionCallbackInterface connection) throws DisconnectException {
        startSameIdChainParse();
        parseRDBRLLBCKreply(connection);
        endOfSameIdChainData();
    }


    public void readLocalXAStart(ConnectionCallbackInterface connection) throws DisconnectException {
    }

    public void readLocalXACommit(ConnectionCallbackInterface connection) throws DisconnectException {
    }

    public void readLocalXARollback(ConnectionCallbackInterface connection) throws DisconnectException {
    }


    protected void readXaStartUnitOfWork(NetConnection conn) throws DisconnectException {
    }

    protected int readXaEndUnitOfWork(NetConnection conn) throws DisconnectException {
        return 0;
    }

    protected int readXaPrepare(NetConnection conn) throws DisconnectException {
        return 0;
    }

    protected void readXaCommit(NetConnection conn) throws DisconnectException {
    }

    protected int readXaRollback(NetConnection conn) throws DisconnectException {
        return 0;
    }

    protected void readXaRecover(NetConnection conn) throws DisconnectException {
    }

    protected void readXaForget(NetConnection conn) throws DisconnectException {
    }


    //------------------parse reply for specific command--------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // Parse the reply for the RDB Commit Unit of Work Command.
    // This method handles the parsing of all command replies and reply data
    // for the rdbcmm command.
    private void parseRDBCMMreply(ConnectionCallbackInterface connection) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP != CodePoint.ENDUOWRM && peekCP != CodePoint.SQLCARD) {
            parseCommitError(connection);
            return;
        }

        if (peekCP == CodePoint.ENDUOWRM) {
            parseENDUOWRM(connection);
            peekCP = parseTypdefsOrMgrlvlovrs();
        }

        NetSqlca netSqlca = parseSQLCARD(null);
        connection.completeSqlca(netSqlca);
    }

    // Parse the reply for the RDB Rollback Unit of Work Command.
    // This method handles the parsing of all command replies and reply data
    // for the rdbrllbck command.
    private void parseRDBRLLBCKreply(ConnectionCallbackInterface connection) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();
        if (peekCP != CodePoint.ENDUOWRM) {
            parseRollbackError();
            return;
        }

        parseENDUOWRM(connection);
        peekCP = parseTypdefsOrMgrlvlovrs();

        NetSqlca netSqlca = parseSQLCARD(null);
        connection.completeSqlca(netSqlca);
    }

    // Parse the reply for the Exchange Server Attributes Command.
    // This method handles the parsing of all command replies and reply data
    // for the excsat command.
    private void parseEXCSATreply(NetConnection netConnection) throws DisconnectException {
        if (peekCodePoint() != CodePoint.EXCSATRD) {
            parseExchangeServerAttributesError();
            return;
        }
        parseEXCSATRD(netConnection);
    }

    // Parse the reply for the Exchange Server Attributes Command (Dummy)
    // This method handles the parsing of all command replies and reply data
    // for the excsat command.
    private void parseDummyEXCSATreply(NetConnection netConnection) throws DisconnectException {
        if (peekCodePoint() != CodePoint.EXCSATRD) {
            parseExchangeServerAttributesError();
            return;
        }
        parseDummyEXCSATRD(netConnection);
    }

    // Parse the reply for the Access Security Command.
    // This method handles the parsing of all command replies and reply data
    // for the accsec command.
    private void parseACCSECreply(NetConnection netConnection, int securityMechanism) throws DisconnectException {
        int peekCP = peekCodePoint();
        if (peekCP != CodePoint.ACCSECRD) {
            parseAccessSecurityError(netConnection);
            return;
        }
        parseACCSECRD(netConnection, securityMechanism);

        peekCP = peekCodePoint();
        if (peekCP == Reply.END_OF_SAME_ID_CHAIN) {
            return;
        }

    }

    // Parse the reply for the Security Check Command.
    // This method handles the parsing of all command replies and reply data
    // for the secchk command.
    private void parseSECCHKreply(NetConnection netConnection) throws DisconnectException {
        if (peekCodePoint() != CodePoint.SECCHKRM) {
            parseSecurityCheckError(netConnection);
            return;
        }

        parseSECCHKRM(netConnection);
        if (peekCodePoint() == CodePoint.SECTKN) {
            // rpydta used only if the security mechanism returns
            // a security token that must be sent back to the source system.
            // this is only used for DCSSEC.  In the case of DCESEC,
            // the sectkn must be returned as reply data if DCE is using
            // mutual authentication.
            // Need to double check what to map this to.  This is probably
            // incorrect but consider it a conversation protocol error
            // 0x03 - OBJDSS sent when not allowed.
            //parseSECTKN (true);
            boolean done = false;
            byte[] bytes = parseSECTKN(false);
        }
    }

    // Parse the reply for the Access RDB Command.
    // This method handles the parsing of all command replies and reply data
    // for the accrdb command.
    private void parseACCRDBreply(NetConnection netConnection) throws DisconnectException {
        int peekCP = peekCodePoint();
        if (peekCP != CodePoint.ACCRDBRM) {
            parseAccessRdbError(netConnection);
            return;
        }

        parseACCRDBRM(netConnection);
        peekCP = peekCodePoint();
        if (peekCP == Reply.END_OF_SAME_ID_CHAIN) {
            return;
        }

        parseTypdefsOrMgrlvlovrs();
        NetSqlca netSqlca = parseSQLCARD(null);
        netConnection.completeSqlca(netSqlca);
    }


    protected int parseTypdefsOrMgrlvlovrs() throws DisconnectException {
        boolean targetTypedefCloned = false;
        while (true) {
            int peekCP = peekCodePoint();
            if (peekCP == CodePoint.TYPDEFNAM) {
                if (!targetTypedefCloned) {
                    netAgent_.targetTypdef_ = (Typdef) netAgent_.targetTypdef_.clone();
                    targetTypedefCloned = true;
                }
                parseTYPDEFNAM();
            } else if (peekCP == CodePoint.TYPDEFOVR) {
                if (!targetTypedefCloned) {
                    netAgent_.targetTypdef_ = (Typdef) netAgent_.targetTypdef_.clone();
                    targetTypedefCloned = true;
                }
                parseTYPDEFOVR();
            } else {
                return peekCP;
            }
        }
    }


    //-----------------------------parse DDM Reply Messages-----------------------

    protected void parseCommitError(ConnectionCallbackInterface connection) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            NetSqlca sqlca = parseAbnormalEndUow(connection);
            connection.completeSqlca(sqlca);
            break;
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        default:
            parseCommonError(peekCP);
            break;
        }
    }

    void parseRollbackError() throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        default:
            parseCommonError(peekCP);
            break;
        }
    }

    void parseExchangeServerAttributesError() throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.MGRLVLRM:
            parseMGRLVLRM();
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void parseAccessSecurityError(NetConnection netConnection) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.RDBNFNRM:
            parseRDBNFNRM(netConnection);
            break;
        case CodePoint.RDBAFLRM:
            parseRdbAccessFailed(netConnection);
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void parseSecurityCheckError(NetConnection netConnection) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.RDBNFNRM:
            parseRDBNFNRM(netConnection);
            break;
        case CodePoint.RDBAFLRM:
            parseRdbAccessFailed(netConnection);
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void parseAccessRdbError(NetConnection netConnection) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.RDBACCRM:
            parseRDBACCRM();
            break;
        case CodePoint.RDBAFLRM:
            parseRdbAccessFailed(netConnection);
            break;
        case CodePoint.RDBATHRM:
            parseRDBATHRM(netConnection);
            break;
        case CodePoint.RDBNFNRM:
            parseRDBNFNRM(netConnection);
            break;
        default:
            parseCommonError(peekCP);
        }
    }


    // Called by all the NET*Reply classes.
    void parseCommonError(int peekCP) throws DisconnectException {
        switch (peekCP) {
        case CodePoint.CMDNSPRM:
            parseCMDNSPRM();
            break;
        case CodePoint.PRCCNVRM:
            parsePRCCNVRM();
            break;
        case CodePoint.SYNTAXRM:
            parseSYNTAXRM();
            break;
        case CodePoint.VALNSPRM:
            parseVALNSPRM();
            break;
        default:
            doObjnsprmSemantics(peekCP);
        }
    }

    NetSqlca parseAbnormalEndUow(ConnectionCallbackInterface connection) throws DisconnectException {
        parseABNUOWRM(connection);
        if (peekCodePoint() != CodePoint.SQLCARD) {
            parseTypdefsOrMgrlvlovrs();
        }

        NetSqlca netSqlca = parseSQLCARD(null);
        return netSqlca;
    }

    void parseRdbAccessFailed(NetConnection netConnection) throws DisconnectException {
        parseRDBAFLRM();

        // an SQLCARD is returned if an RDBALFRM is returned.
        // this SQLCARD always follows the RDBALFRM.
        // TYPDEFNAM and TYPDEFOVR are MTLINC

        if (peekCodePoint() == CodePoint.TYPDEFNAM) {
            parseTYPDEFNAM();
            parseTYPDEFOVR();
        } else {
            parseTYPDEFOVR();
            parseTYPDEFNAM();
        }

        NetSqlca netSqlca = parseSQLCARD(null);
        
        //Check if the SQLCARD has null SQLException
        if(netSqlca.getSqlErrmc() == null)
        	netConnection.setConnectionNull(true);
        else
        	netConnection.completeSqlca(netSqlca);
    }


    // The Security Check (SECCHKRM) Reply Message indicates the acceptability
    // of the security information.
    // this method returns the security check code. it is up to the caller to check
    // the value of this return code and take the appropriate action.
    //
    // Returned from Server:
    // SVRCOD - required  (0 - INFO, 8 - ERROR, 16 -SEVERE)
    // SECCHKCD - required
    // SECTKN - optional, ignorable
    // SVCERRNO - optional
    private void parseSECCHKRM(NetConnection netConnection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean secchkcdReceived = false;
        int secchkcd = CodePoint.SECCHKCD_00;
        boolean sectknReceived = false;
        byte[] sectkn = null;

        parseLengthAndMatchCodePoint(CodePoint.SECCHKRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                // severity code.  it's value is dictated by the SECCHKCD.
                // right now it will not be checked that it is the correct value
                // for the SECCHKCD.  maybe this will be done in the future.
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_SEVERE);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECCHKCD) {
                // security check code. this specifies the state of the security information.
                // there is a relationship between this value and the SVRCOD value.
                // right now this driver will not check these values against each other.
                foundInPass = true;
                secchkcdReceived = checkAndGetReceivedFlag(secchkcdReceived);
                secchkcd = parseSECCHKCD();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECTKN) {
                // security token.
                // used when mutual authentication of the source and target servers
                // is requested.  The architecture lists this as an instance variable
                // and also says that the SECTKN flows as reply data to the secchk cmd and
                // it must flow after the secchkrm message.  Right now this driver doesn't
                // support ay mutual authentication so it will be ignored (it is listed
                // as an ignorable instance variable in the ddm manual).
                foundInPass = true;
                sectknReceived = checkAndGetReceivedFlag(sectknReceived);
                sectkn = parseSECTKN(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        // check for the required instance variables.
        checkRequiredObjects(svrcodReceived, secchkcdReceived);

        netConnection.securityCheckComplete(svrcod, secchkcd);
    }


    // Access to RDB Completed (ACRDBRM) Reply Message specifies that an
    // instance of the SQL application manager has been created and is bound
    // to the specified relation database (RDB).
    //
    // Returned from Server:
    // SVRCOD - required  (0 - INFO, 4 - WARNING)
    // PRDID - required
    // TYPDEFNAM - required (MINLVL 4) (QTDSQLJVM)
    // TYPDEFOVR - required
    // RDBINTTKN - optional
    // CRRTKN - optional
    // USRID - optional
    // SRVLST - optional (MINLVL 5)
    private void parseACCRDBRM(NetConnection netConnection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean prdidReceived = false;
        String prdid = null;
        boolean typdefnamReceived = false;
        boolean typdefovrReceived = false;
        boolean rdbinttknReceived = false;
        boolean crrtknReceived = false;
        byte[] crrtkn = null;
        boolean usridReceived = false;
        String usrid = null;

        parseLengthAndMatchCodePoint(CodePoint.ACCRDBRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                // severity code.  If the target SQLAM cannot support the typdefovr
                // parameter values specified for the double-byte and mixed-byte CCSIDs
                // on the corresponding ACCRDB command, then the severity code WARNING
                // is specified on the ACCRDBRM.
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_WARNING);
                peekCP = peekCodePoint();
            }

            // this is the product release level of the target RDB server.
            if (peekCP == CodePoint.PRDID) {
                foundInPass = true;
                prdidReceived = checkAndGetReceivedFlag(prdidReceived);
                prdid = parsePRDID(false); // false means do not skip the bytes
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.TYPDEFNAM) {
                // this is the name of the data type to the data representation mapping
                // definitions tha the target SQLAM uses when sending reply data objects.
                foundInPass = true;
                typdefnamReceived = checkAndGetReceivedFlag(typdefnamReceived);
                parseTYPDEFNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.TYPDEFOVR) {
                // this is the single-byte, double-byte, and mixed-byte CCSIDs of the
                // scalar data arrays (SDA) in the identified data type to data representation
                // mapping definitions.
                foundInPass = true;
                typdefovrReceived = checkAndGetReceivedFlag(typdefovrReceived);
                parseTYPDEFOVR();
                peekCP = peekCodePoint();
            }


            if (peekCP == CodePoint.USRID) {
                // specifies the target defined user ID.  It is returned if the value of
                // TRGDFTRT is TRUE in ACCRDB.  Right now this driver always sets this
                // value to false so this should never get returned here.
                // if it is returned, it could be considered an error but for now
                // this driver will just skip the bytes.
                foundInPass = true;
                usridReceived = checkAndGetReceivedFlag(usridReceived);
                usrid = parseUSRID(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CRRTKN) {
                // carries information to correlate with the work being done on bahalf
                // of an application at the source and at the target server.
                // defualt value is ''.
                // this parameter is only retunred if an only if the CRRTKN parameter
                // is not received on ACCRDB.  We will rely on server to send us this
                // in ACCRDBRM
                foundInPass = true;
                crrtknReceived = checkAndGetReceivedFlag(crrtknReceived);
                crrtkn = parseCRRTKN(false);
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        // check for the required instance variables.
        checkRequiredObjects(svrcodReceived,
                prdidReceived,
                typdefnamReceived,
                typdefovrReceived);

        netConnection.rdbAccessed(svrcod,
                prdid,
                crrtknReceived,
                crrtkn);
    }


    // The End Unit of Work Condition (ENDUOWRM) Reply Mesage specifies
    // that the unit of work has ended as a result of the last command.
    //
    // Returned from Server:
    //   SVRCOD - required  (4 WARNING)
    //   UOWDSP - required
    //   RDBNAM - optional
    void parseENDUOWRM(ConnectionCallbackInterface connection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean uowdspReceived = false;
        int uowdsp = 0;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.ENDUOWRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_WARNING, CodePoint.SVRCOD_WARNING);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.UOWDSP) {
                foundInPass = true;
                uowdspReceived = checkAndGetReceivedFlag(uowdspReceived);
                uowdsp = parseUOWDSP();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, uowdspReceived);

        netAgent_.setSvrcod(svrcod);
        if (uowdsp == CodePoint.UOWDSP_COMMIT) {
            connection.completeLocalCommit();
        } else {
            connection.completeLocalRollback();
        }
    }

    // Command Check Reply Message indicates that the requested
    // command encountered an unarchitected and implementation-specific
    // condition for which there is no architected message.  If the severity
    // code value is ERROR or greater, the command has failed.  The
    // message can be accompanied by other messages that help to identify
    // the specific condition.
    // The CMDCHKRM should not be used as a general catch-all in place of
    // product-defined messages when using product extensions to DDM.
    // PROTOCOL architects the SQLSTATE value depending on SVRCOD
    // SVRCOD 0 -> SQLSTATE is not returned
    // SVRCOD 8 -> SQLSTATE of 58008 or 58009
    // SVRCOD 16,32,64,128 -> SQLSTATE of 58009
    //
    // Messages
    //   SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //       Error that will affect the successful execution of subsequent
    //       commands and SQL statements: Reason Code <reason-code>.
    //     Some possible reason codes include:
    //       121C Indicates that the user is not authorized to perform the requested command.
    //       1232 The command could not be completed because of a permanent error.
    //         In most cases, the server will be in the process of an abend.
    //       220A The target server has received an invalid data description.
    //         If a user SQLDA is specified, ensure that the fields are
    //         initialized correctly. Also, ensure that the length does not
    //         exceed the maximum allowed length for the data type being used.
    //
    // The command or statement cannot be processed.  The current
    // transaction is rolled back and the application is disconnected
    //  from the remote database.
    //
    //
    // Returned from Server:
    //   SVRCOD - required  (0 - INFO, 4 - WARNING, 8 - ERROR, 16 - SEVERE,
    //                       32 - ACCDMG, 64 - PRMDMG, 128 - SESDMG))
    //   RDBNAM - optional (MINLVL 3)
    //   RECCNT - optional (MINVAL 0, MINLVL 3)
    //
    // Called by all the Reply classesCMDCHKRM
    protected void parseCMDCHKRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        parseLengthAndMatchCodePoint(CodePoint.CMDCHKRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_SESDMG);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }
            // skip over the RECCNT since it can't be found in the DDM book.

            if (peekCP == 0x115C) {
                foundInPass = true;
                parseLengthAndMatchCodePoint(0x115C);
                skipBytes();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "The requested command encountered an unarchitected and implementation " +
                "specific condition for which there was no architected message.",
                SqlState._58009));
    }


    // RDB Not Accessed Reply Message indicates that the access relational
    // database command (ACCRDB) was not issued prior to a command
    // requesting the RDB Services.
    // PROTOCOL Architects an SQLSTATE of 58008 or 58009.
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //      transaction is rolled back and the application is disconnected
    //      from the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - required
    //
    // Called by all the NET*Reply classes.
    void parseRDBNACRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBNACRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "The access relational database command was not issued prior to " +
                "a command requesting RDB services.  ",
                SqlState._58009));
    }

    // RDB Not Found Reply Message indicates that the target
    // server cannot find the specified relational database.
    // PROTOCOL architects an SQLSTATE of 08004.
    //
    // Messages
    // SQLSTATE : 8004
    //     The application server rejected establishment of the connection.
    //     SQLCODE : -30061
    //     The database alias or database name <name> was not found at the remote node.
    //     The statement cannot be processed.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - required
    //
    private void parseRDBNFNRM(NetConnection netConnection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBNFNRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "The application server rejected establishment of the connection.  " +
                "An attempt was made to access a database, " +
                netConnection.databaseName_ + ", which was not found.",
                SqlState._08004));
    }


    // Not Authorized to RDB Reply Message specifies that
    // the requester is not authorized to access the specified
    // relational database.
    // PROTOCOL architects an SQLSTATE of 08004
    //
    // Messages
    // SQLSTATE : 8004
    //     Authorization ID <authorization-ID> attempted to perform the specified
    //     <operation> without having been granted the proper authorization to do so.
    //     SQLCODE : -30060
    //      <authorization-ID> does not have the privilege to perform operation <operation>.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - required
    //
    private void parseRDBATHRM(NetConnection netConnection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBATHRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        netAgent_.setSvrcod(svrcod);
        netAgent_.accumulateReadException(new SqlException(agent_.logWriter_,
                "The application server rejected establishment of the connection.  " +
                "The user is not authorized to access the database.",
                SqlState._08004));
    }

    // Data Stream Syntax Error Reply Message indicates that the data
    // sent to the target agent does not structurally conform to the requirements
    // of the DDM architecture.  The target agent terminated paring of the DSS
    // when the condition SYNERRCD specified was detected.
    // PROTOCOL architects an SQLSTATE of 58008 or 58009.
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //          transaction is rolled back and the application is disconnected
    //          from the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // SYNERRCD - required
    // RECCNT - optional (MINVAL 0, MINLVL 3) (will not be returned - should be ignored)
    // CODPNT - optional (MINLVL 3)
    // RDBNAM - optional (MINLVL 3)
    //
    protected void parseSYNTAXRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean synerrcdReceived = false;
        int synerrcd = 0;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean codpntReceived = false;
        int codpnt = 0;

        parseLengthAndMatchCodePoint(CodePoint.SYNTAXRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SYNERRCD) {
                foundInPass = true;
                synerrcdReceived = checkAndGetReceivedFlag(synerrcdReceived);
                synerrcd = parseSYNERRCD();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CODPNT) {
                foundInPass = true;
                codpntReceived = checkAndGetReceivedFlag(codpntReceived);
                codpnt = parseCODPNT();
                peekCP = peekCodePoint();
            }

            // RECCNT will be skipped.

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, synerrcdReceived);

        netAgent_.setSvrcod(svrcod);
        doSyntaxrmSemantics(codpnt);
    }

    // RDB Currently Accessed Reply Message inidcates that the
    // ACCRDB command cannot be issued because the requester
    // has access to a relational database.
    // PROTOCOL architects an SQLSTATE of 58008 or 58009.
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //      transaction is rolled back and the application is disconnected
    //      from the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - required
    //
    private void parseRDBACCRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBACCRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "The access relational database command cannot be issued because an " +
                "RDB is already currently accessed.",
                SqlState._58009));
    }

    // RDB Access Failed Reply Message specifies that the relational
    // database failed the attempted connection.
    // An SQLCARD object must also be returned, following the
    // RDBAFLRM, to explain why the RDB failed the connection.
    // In addition, the target SQLAM instance is destroyed.
    // The SQLSTATE is returned in the SQLCARD.
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //      transaction is rolled back and the application is disconnected
    //      from the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - required
    //
    private void parseRDBAFLRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBAFLRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        netAgent_.setSvrcod(svrcod);
    }


    // Parameter Value Not Supported Reply Message indicates
    // that the parameter value specified is either not recognized
    // or not supported for the specified parameter.
    // The VALNSPRM can only be specified in accordance with
    // the rules specified for DDM subsetting.
    // The code point of the command parameter in error is
    // returned as a parameter in this message.
    // PROTOCOL Architects an SQLSTATE of 58017.
    //
    // if codepoint is 0x119C,0x119D, or 0x119E then SQLSTATE 58017, SQLCODE -332
    // else SQLSTATE 58017, SQLCODE -30073
    //
    // Messages
    // SQLSTATE : 58017
    //     The DDM parameter value is not supported.
    //     SQLCODE : -332
    //     There is no available conversion for the source code page
    //         <code page> to the target code page <code page>.
    //         Reason code <reason-code>.
    //     The reason codes are as follows:
    //     1 source and target code page combination is not supported
    //         by the database manager.
    //     2 source and target code page combination is either not
    //         supported by the database manager or by the operating
    //         system character conversion utility on the client node.
    //     3 source and target code page combination is either not
    //         supported by the database manager or by the operating
    //         system character conversion utility on the server node.
    //
    // SQLSTATE : 58017
    //     The DDM parameter value is not supported.
    //     SQLCODE : -30073
    //     <parameter-identifier> Parameter value <value> is not supported.
    //     Some possible parameter identifiers include:
    //     002F  The target server does not support the data type
    //         requested by the application requester.
    //         The target server does not support the CCSID
    //         requested by the application requester. Ensure the CCSID
    //         used by the requester is supported by the server.
    //         119C - Verify the single-byte CCSID.
    //         119D - Verify the double-byte CCSID.
    //         119E - Verify the mixed-byte CCSID.
    //
    //     The current environment command or SQL statement
    //         cannot be processed successfully, nor can any subsequent
    //         commands or SQL statements.  The current transaction is
    //         rolled back and the application is disconnected
    //         from the remote database. The command cannot be processed.
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // CODPNT - required
    // RECCNT - optional (MINLVL 3, MINVAL 0) (will not be returned - should be ignored)
    // RDBNAM - optional (MINLVL 3)
    //
    protected void parseVALNSPRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean codpntReceived = false;
        int codpnt = 0;

        parseLengthAndMatchCodePoint(CodePoint.VALNSPRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CODPNT) {
                foundInPass = true;
                codpntReceived = checkAndGetReceivedFlag(codpntReceived);
                codpnt = parseCODPNT();
                peekCP = peekCodePoint();
            }

            // RECCNT will be skipped

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, codpntReceived);

        netAgent_.setSvrcod(svrcod);
        doValnsprmSemantics(codpnt, "\"\"");
    }


    // Conversational Protocol Error Reply Message
    // indicates that a conversational protocol error occurred.
    // PROTOCOL architects the SQLSTATE value depending on SVRCOD
    // SVRCOD 8 -> SQLSTATE of 58008 or 58009
    // SVRCOD 16,128 -> SQLSTATE of 58009
    //
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //      transaction is rolled back and the application is disconnected
    //      from the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR, 16 - SEVERE, 128 - SESDMG)
    // PRCCNVCD - required
    // RECCNT - optional (MINVAL 0, MINLVL 3)
    // RDBNAM - optional (NINLVL 3)
    //
    protected void parsePRCCNVRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean prccnvcdReceived = false;
        int prccnvcd = 0;

        parseLengthAndMatchCodePoint(CodePoint.PRCCNVRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_SESDMG);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.PRCCNVCD) {
                foundInPass = true;
                prccnvcdReceived = checkAndGetReceivedFlag(prccnvcdReceived);
                prccnvcd = parsePRCCNVCD();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, prccnvcdReceived);

        netAgent_.setSvrcod(svrcod);
        doPrccnvrmSemantics(CodePoint.PRCCNVRM);
    }

    // Object Not Supported Reply Message indicates that the target
    // server does not recognize or support the object
    // specified as data in an OBJDSS for the command associated
    // with the object.
    // The OBJNSPRM is also returned if an object is found in a
    // valid collection in an OBJDSS (such as RECAL collection)
    // that that is not valid for that collection.
    // PROTOCOL Architects an SQLSTATE of 58015.
    //
    // Messages
    // SQLSTATE : 58015
    //     The DDM object is not supported.
    //     SQLCODE : -30071
    //      <object-identifier> Object is not supported.
    //     The current transaction is rolled back and the application
    //     is disconnected from the remote database. The command
    //     cannot be processed.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR, 16 - SEVERE)
    // CODPNT - required
    // RECCNT - optional (MINVAL 0)  (will not be returned - should be ignored)
    // RDBNAM - optional (MINLVL 3)
    //
    // Also called by NetPackageReply and NetStatementReply
    void parseOBJNSPRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean codpntReceived = false;
        int codpnt = 0;

        parseLengthAndMatchCodePoint(CodePoint.OBJNSPRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_SEVERE);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CODPNT) {
                foundInPass = true;
                codpntReceived = checkAndGetReceivedFlag(codpntReceived);
                codpnt = parseCODPNT();
                peekCP = peekCodePoint();
            }

            // skip the RECCNT

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, codpntReceived);

        netAgent_.setSvrcod(svrcod);
        doObjnsprmSemantics(codpnt);
    }


    // Manager-Level Conflict (MGRLVLRM) Reply Message indicates that
    // the manager levels specified in the MGRLVLLS conflict amoung
    // themselves or with previously specified manager levels.
    // - The manager-level dependencies of one specified manager violates another
    //   specified maanger level.
    // - The manager- level specified attempts to respecify a manager level that
    //   previously EXCSAT command specified.
    // PROTOCOL architects an SQLSTATE of 58010.
    //
    // Messages
    // SQLSTATE : 58010
    //     Execution failed due to a distributed protocol error that will affect
    //     the successful execution of subsequent DDM commands or SQL statements.
    //     SQLCODE : -30021
    //     Execution failed due to a distribution protocol error
    //     that will affect the successful execution of subsequent
    //     commands and SQL statements: Manager <manager> at Level <level>
    //     not supported.
    //
    //     A system error occurred that prevented successful connection
    //     of the application to the remote database.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // MGRLVLLS - required
    //
    private void parseMGRLVLRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean mgrlvllsReceived = false;
        int[] managerCodePoint = null;
        int[] managerLevel = null;

        parseLengthAndMatchCodePoint(CodePoint.MGRLVLRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.MGRLVLLS) {
                foundInPass = true;
                mgrlvllsReceived = checkAndGetReceivedFlag(mgrlvllsReceived);

                parseLengthAndMatchCodePoint(CodePoint.MGRLVLLS);
                int managerListLength = getDdmLength();
                if ((managerListLength == 0) || ((managerListLength % 7) != 0)) {
                    doSyntaxrmSemantics(CodePoint.SYNERRCD_OBJ_LEN_NOT_ALLOWED);
                }

                int managerCount = managerListLength / 7;
                managerCodePoint = new int[managerCount];
                managerLevel = new int[managerCount];
                for (int i = 0; i < managerCount; i++) {
                    managerCodePoint[i] = parseCODPNTDR();
                    managerLevel[i] = parseMGRLVLN();
                }
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, mgrlvllsReceived);

        netAgent_.setSvrcod(svrcod);
        doMgrlvlrmSemantics(managerCodePoint, managerLevel);
    }


    // Command Not Supported Reply Message indicates that the specified
    // command is not recognized or not supported for the
    // specified target.  The reply message can be returned
    // only in accordance with the architected rules for DDM subsetting.
    // PROTOCOL architects an SQLSTATE of 58014.
    //
    // Messages
    // SQLSTATE : 58014
    //     The DDM command is not supported.
    //     SQLCODE : -30070
    //      <command-identifier> Command is not supported.
    //     The current transaction is rolled back and the application is
    //     disconnected from the remote database. The statement cannot be processed.
    //
    //
    // Returned from Server:
    // SVRCOD - required  (4 - WARNING, 8 - ERROR) (MINLVL 2)
    // CODPNT - required
    // RDBNAM - optional (MINLVL 3)
    //
    protected void parseCMDNSPRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean srvdgnReceived = false;
        byte[] srvdgn = null;
        boolean codpntReceived = false;
        int codpnt = 0;

        parseLengthAndMatchCodePoint(CodePoint.CMDNSPRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_WARNING, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }


            if (peekCP == CodePoint.CODPNT) {
                foundInPass = true;
                codpntReceived = checkAndGetReceivedFlag(codpntReceived);
                codpnt = parseCODPNT();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, codpntReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "The DDM command is not supported.  " +
                "Unsupported DDM command code point: " +
                "0x" + Integer.toHexString(codpnt),
                SqlState._58014));
    }

    // Abnormal End Unit of Work Condition Reply Message indicates
    // that the current unit of work ended abnormally because
    // of some action at the target server.  This can be caused by a
    // deadlock resolution, operator intervention, or some similar
    // situation that caused the relational database to rollback
    // the current unit of work.  This reply message is returned only
    // if an SQLAM issues the command.  Whenever an ABNUOWRM is returned
    // in response to a command, an SQLCARD object must also be returned
    // following the ABNUOWRM.  The SQLSTATE is returned in the SQLCARD.
    //
    // Returned from Server:
    //   SVRCOD - required (8 - ERROR)
    //   RDBNAM - required
    //
    // Called by all the NET*Reply classes.
    void parseABNUOWRM(ConnectionCallbackInterface connection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.ABNUOWRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.RDBNAM) {
                // skip the rbbnam since it doesn't tell us anything new.
                // there is no way to return it to the application anyway.
                // not having to convert this to a string is a time saver also.
                foundInPass = true;
                rdbnamReceived = checkAndGetReceivedFlag(rdbnamReceived);
                rdbnam = parseRDBNAM(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }

        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived);

        // the abnuowrm has been received, do whatever state changes are necessary
        netAgent_.setSvrcod(svrcod);
        connection.completeAbnormalUnitOfWork();

    }

    //--------------------- parse DDM Reply Data--------------------------------------

    // The Server Attributes Reply Data (EXCSATRD) returns the following
    // information in response to an EXCSAT command:
    // - the target server's class name
    // - the target server's support level for each class of manager
    //   the source requests
    // - the target server's product release level
    // - the target server's external name
    // - the target server's name
    //
    // Returned from Server:
    // EXTNAM - optional
    // MGRLVLLS - optional
    // SRVCLSNM - optional
    // SRVNAM - optional
    // SRVRLSLV - optional
    private void parseEXCSATRD(NetConnection netConnection) throws DisconnectException {
        boolean extnamReceived = false;
        String extnam = null;
        boolean mgrlvllsReceived = false;
        boolean srvclsnmReceived = false;
        String srvclsnm = null;
        boolean srvnamReceived = false;
        String srvnam = null;
        boolean srvrlslvReceived = false;
        String srvrlslv = null;

        parseLengthAndMatchCodePoint(CodePoint.EXCSATRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.EXTNAM) {
                // External Name is the name of the job, task, or process
                // on a system for which a DDM server is active.  For a target
                // DDM server, the external name is the name of the job the system creates
                // or activates to run the DDM server.
                // No semantic meaning is assigned to external names in DDM.
                // External names are transmitted to aid in problem determination.
                // This driver will save the external name of the target (the
                // driver may use it for logging purposes later).
                foundInPass = true;
                extnamReceived = checkAndGetReceivedFlag(extnamReceived);
                extnam = parseEXTNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.MGRLVLLS) {
                // Manager-Level List
                // specifies a list of code points and support levels for the
                // classes of managers a server supports
                foundInPass = true;
                mgrlvllsReceived = checkAndGetReceivedFlag(mgrlvllsReceived);
                parseMGRLVLLS(netConnection);  // need to review this one, check input and output
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVCLSNM) {
                // Server Class Name
                // specifies the name of a class of ddm servers.
                foundInPass = true;
                srvclsnmReceived = checkAndGetReceivedFlag(srvclsnmReceived);
                srvclsnm = parseSRVCLSNM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVNAM) {
                // Server Name
                // no semantic meaning is assigned to server names in DDM,
                // it is recommended (by the DDM manual) that the server's
                // physical or logical location identifier be used as a server name.
                // server names are transmitted for problem determination purposes.
                // this driver will save this name and in the future may use it
                // for logging errors.
                foundInPass = true;
                srvnamReceived = checkAndGetReceivedFlag(srvnamReceived);
                srvnam = parseSRVNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVRLSLV) {
                // Server Product Release Level
                // specifies the procuct release level of a ddm server.
                // the contents are unarchitected.
                // this driver will save this information and in the future may
                // use it for logging purposes.
                foundInPass = true;
                srvrlslvReceived = checkAndGetReceivedFlag(srvrlslvReceived);
                srvrlslv = parseSRVRLSLV();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        // according the the DDM book, all these instance variables are optional
        netConnection.setServerAttributeData(extnam, srvclsnm, srvnam, srvrlslv);
    }

    // Must make a version that does not change state in the associated connection
    private void parseDummyEXCSATRD(NetConnection netConnection) throws DisconnectException {
        boolean extnamReceived = false;
        String extnam = null;
        boolean mgrlvllsReceived = false;
        boolean srvclsnmReceived = false;
        String srvclsnm = null;
        boolean srvnamReceived = false;
        String srvnam = null;
        boolean srvrlslvReceived = false;
        String srvrlslv = null;

        parseLengthAndMatchCodePoint(CodePoint.EXCSATRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.EXTNAM) {
                // External Name is the name of the job, task, or process
                // on a system for which a DDM server is active.  For a target
                // DDM server, the external name is the name of the job the system creates
                // or activates to run the DDM server.
                // No semantic meaning is assigned to external names in DDM.
                // External names are transmitted to aid in problem determination.
                // This driver will save the external name of the target (the
                // driver may use it for logging purposes later).
                foundInPass = true;
                extnamReceived = checkAndGetReceivedFlag(extnamReceived);
                extnam = parseEXTNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.MGRLVLLS) {
                // Manager-Level List
                // specifies a list of code points and support levels for the
                // classes of managers a server supports
                foundInPass = true;
                mgrlvllsReceived = checkAndGetReceivedFlag(mgrlvllsReceived);
                parseMGRLVLLS(netConnection);  // need to review this one, check input and output
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVCLSNM) {
                // Server Class Name
                // specifies the name of a class of ddm servers.
                foundInPass = true;
                srvclsnmReceived = checkAndGetReceivedFlag(srvclsnmReceived);
                srvclsnm = parseSRVCLSNM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVNAM) {
                // Server Name
                // no semantic meaning is assigned to server names in DDM,
                // it is recommended (by the DDM manual) that the server's
                // physical or logical location identifier be used as a server name.
                // server names are transmitted for problem determination purposes.
                // this driver will save this name and in the future may use it
                // for logging errors.
                foundInPass = true;
                srvnamReceived = checkAndGetReceivedFlag(srvnamReceived);
                srvnam = parseSRVNAM();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SRVRLSLV) {
                // Server Product Release Level
                // specifies the procuct release level of a ddm server.
                // the contents are unarchitected.
                // this driver will save this information and in the future may
                // use it for logging purposes.
                foundInPass = true;
                srvrlslvReceived = checkAndGetReceivedFlag(srvrlslvReceived);
                srvrlslv = parseSRVRLSLV();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        // according the the DDM book, all these instance variables are optional
        // don't change state of netConnection because this is a DUMMY flow
        //netConnection.setServerAttributeData (extnam, srvclsnm, srvnam, srvrlslv);
    }

    // The Access Security Reply Data (ACSECRD) Collection Object contains
    // the security information from a target server's security manager.
    // this method returns the security check code received from the server
    // (if the server does not return a security check code, this method
    // will return 0).  it is up to the caller to check
    // the value of this return code and take the appropriate action.
    //
    // Returned from Server:
    // SECMEC - required
    // SECTKN - optional (MINLVL 6)
    // SECCHKCD - optional
    private void parseACCSECRD(NetConnection netConnection, int securityMechanism) throws DisconnectException {
        boolean secmecReceived = false;
        int[] secmecList = null;
        boolean sectknReceived = false;
        byte[] sectkn = null;
        boolean secchkcdReceived = false;
        int secchkcd = 0;

        parseLengthAndMatchCodePoint(CodePoint.ACCSECRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SECMEC) {
                // security mechanism.
                // this value must either reflect the value sent in the ACCSEC command
                // if the target server supports it; or the values the target server
                // does support when it does not support or accept the value
                // requested by the source server.
                // the secmecs returned are treated as a list and stored in
                // targetSecmec_List.
                // if the target server supports the source's secmec, it
                // will be saved in the variable targetSecmec_ (NOTE: so
                // after calling this method, if targetSecmec_'s value is zero,
                // then the target did NOT support the source secmec.  any alternate
                // secmecs would be contained in targetSecmec_List).
                foundInPass = true;
                secmecReceived = checkAndGetReceivedFlag(secmecReceived);
                secmecList = parseSECMEC();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECTKN) {
                // security token
                foundInPass = true;
                sectknReceived = checkAndGetReceivedFlag(sectknReceived);
                sectkn = parseSECTKN(false);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SECCHKCD) {
                // security check code.
                // included if and only if an error is detected when processing
                // the ACCSEC command.  this has an implied severity code
                // of ERROR.
                foundInPass = true;
                secchkcdReceived = checkAndGetReceivedFlag(secchkcdReceived);
                secchkcd = parseSECCHKCD();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();
        checkRequiredObjects(secmecReceived);

        netConnection.setAccessSecurityData(secchkcd,
                securityMechanism,
                secmecList,
                sectknReceived,
                sectkn);
    }

    // Called by all the NET*Reply classes.
    void parseTYPDEFNAM() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.TYPDEFNAM);
        netAgent_.targetTypdef_.setTypdefnam(readString());
    }

    // Called by all the NET*Reply classes.
    void parseTYPDEFOVR() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.TYPDEFOVR);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.CCSIDSBC) {
                foundInPass = true;
                netAgent_.targetTypdef_.setCcsidSbc(parseCCSIDSBC());
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CCSIDDBC) {
                foundInPass = true;
                netAgent_.targetTypdef_.setCcsidDbc(parseCCSIDDBC());
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.CCSIDMBC) {
                foundInPass = true;
                netAgent_.targetTypdef_.setCcsidMbc(parseCCSIDMBC());
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
    }


    // The SYNCCRD Reply Mesage
    //
    // Returned from Server:
    //   XARETVAL - required
    int parseSYNCCRD(ConnectionCallbackInterface connection) throws DisconnectException {
        return 0;
    }

    // Process XA return value
    protected int parseXARETVAL() throws DisconnectException {
        return 0;
    }

    // Process XA return value
    protected byte parseSYNCTYPE() throws DisconnectException {
        return 0;
    }

    // This method handles the parsing of all command replies and reply data
    // for the SYNCCTL command.
    protected int parseSYNCCTLreply(ConnectionCallbackInterface connection) throws DisconnectException {
        return 0;
    }

    // Called by the XA commit and rollback parse reply methods.
    void parseSYNCCTLError(int peekCP) throws DisconnectException {
        switch (peekCP) {
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.PRCCNVRM:
            parsePRCCNVRM();
            break;
        case CodePoint.SYNTAXRM:
            parseSYNTAXRM();
            break;
        case CodePoint.VALNSPRM:
            parseVALNSPRM();
            break;
        default:
            doObjnsprmSemantics(peekCP);
        }
    }


    // Manager-Level List.
    // Specifies a list of code points and support levels for the
    // classes of managers a server supports.
    // The target server must not provide information for any target
    // managers unless the source explicitly requests it.
    // For each manager class, if the target server's support level
    // is greater than or equal to the source server's level, then the source
    // server's level is returned for that class if the target server can operate
    // at the source's level; otherwise a level 0 is returned.  If the target
    // server's support level is less than the source server's level, the
    // target server's level is returned for that class.  If the target server
    // does not recognize the code point of a manager class or does not support
    // that class, it returns a level of 0.  The target server then waits
    // for the next command or for the source server to terminate communications.
    // When the source server receives EXCSATRD, it must compare each of the entries
    // in the mgrlvlls parameter it received to the corresponding entries in the mgrlvlls
    // parameter it sent.  If any level mismatches, the source server must decide
    // whether it can use or adjust to the lower level of target support for that manager
    // class.  There are no architectural criteria for making this decision.
    // The source server can terminate communications or continue at the target
    // servers level of support.  It can also attempt to use whatever
    // commands its user requests while receiving eror reply messages for real
    // functional mismatches.
    // The manager levels the source server specifies or the target server
    // returns must be compatible with the manager-level dependencies of the specified
    // manangers.  Incompatible manager levels cannot be specified.
    // After this method successfully returns, the targetXXXX values (where XXXX
    // represents a manager name.  example targetAgent) contain the negotiated
    // manager levels for this particular connection.
    private void parseMGRLVLLS(NetConnection netConnection) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.MGRLVLLS);

        // each manager class and level is 4 bytes long.
        // get the length of the mgrlvls bytes, make sure it contains
        // the correct number of bytes for a mgrlvlls object, and calculate
        // the number of manager's returned from the server.
        int managerListLength = getDdmLength();
        if ((managerListLength == 0) || ((managerListLength % 4) != 0)) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_OBJ_LEN_NOT_ALLOWED);
        }
        int managerCount = managerListLength / 4;

        // the managerCount should be equal to the same number of
        // managers sent on the excsat.

        // read each of the manager levels returned from the server.
        for (int i = 0; i < managerCount; i++) {

            // first two byte are the manager's codepoint, next two bytes are the level.
            int managerCodePoint = parseCODPNTDR();
            int managerLevel = parseMGRLVLN();

            // check each manager to make sure levels are within proper limits
            // for this driver.  Also make sure unexpected managers are not returned.
            switch (managerCodePoint) {

            case CodePoint.AGENT:
                if ((managerLevel < NetConfiguration.MIN_AGENT_MGRLVL) ||
                        (managerLevel > netConnection.targetAgent_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetAgent_ = managerLevel;
                break;

            case CodePoint.CMNTCPIP:
                if ((managerLevel < NetConfiguration.MIN_CMNTCPIP_MGRLVL) ||
                        (managerLevel > netConnection.targetCmntcpip_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetCmntcpip_ = managerLevel;
                break;

            case CodePoint.RDB:
                if ((managerLevel < NetConfiguration.MIN_RDB_MGRLVL) ||
                        (managerLevel > netConnection.targetRdb_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetRdb_ = managerLevel;
                break;

            case CodePoint.SECMGR:
                if ((managerLevel < NetConfiguration.MIN_SECMGR_MGRLVL) ||
                        (managerLevel > netConnection.targetSecmgr_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetSecmgr_ = managerLevel;
                break;

            case CodePoint.SQLAM:
                if ((managerLevel < NetConfiguration.MIN_SQLAM_MGRLVL) ||
                        (managerLevel > netAgent_.targetSqlam_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netAgent_.orignalTargetSqlam_ = managerLevel;
                break;

            case CodePoint.CMNAPPC:
                if ((managerLevel < NetConfiguration.MIN_CMNAPPC_MGRLVL) ||
                        (managerLevel > netConnection.targetCmnappc_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetCmnappc_ = managerLevel;
                break;

            case CodePoint.XAMGR:
                if ((managerLevel != 0) &&
                        (managerLevel < NetConfiguration.MIN_XAMGR_MGRLVL) ||
                        (managerLevel > netConnection.targetXamgr_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetXamgr_ = managerLevel;
                break;

            case CodePoint.SYNCPTMGR:
                if ((managerLevel != 0) &&
                        (managerLevel < NetConfiguration.MIN_SYNCPTMGR_MGRLVL) ||
                        (managerLevel > netConnection.targetSyncptmgr_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetSyncptmgr_ = managerLevel;
                break;

            case CodePoint.RSYNCMGR:
                if ((managerLevel != 0) &&
                        (managerLevel < NetConfiguration.MIN_RSYNCMGR_MGRLVL) ||
                        (managerLevel > netConnection.targetRsyncmgr_)) {
                    doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                }
                netConnection.targetRsyncmgr_ = managerLevel;
                break;
                // The target server must not provide information for any target managers
                // unless the source explicitly requests.  The following managers are never requested.
            default:
                doMgrlvlrmSemantics(managerCodePoint, managerLevel);
                break;
            }
        }
    }

    // The External Name is the name of the job, task, or process on a
    // system for which a DDM server is active.  On a source DDM server,
    // the external name is the name of the job that is requesting
    // access to remote resources.  For a target DDM server,
    // the external name is the name of the job the system
    // creates or activates to run the DDM server.
    // No semantic meaning is assigned to external names in DDM.
    // External names are transmitted to aid in problem determination.
    protected String parseEXTNAM() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.EXTNAM);
        return readString();
    }

    // Server Class name specifies the name of a class of DDM servers.
    // Server class names are assigned for each product involved in PROTOCOL.
    protected String parseSRVCLSNM() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SRVCLSNM);
        return readString();
    }

    // Server Name is the name of the DDM server.
    // No semantic meaning is assigned to server names in DDM,
    // but it is recommended that the server names are transmitted
    // for problem determination.
    protected String parseSRVNAM() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SRVNAM);
        return readString();
    }

    // Server Product Release Level String specifies the product
    // release level of a DDM server.  The contents of the
    // parameter are unarchitected.  Up to 255 bytes can be sent.
    // SRVRLSLV should not be used in place of product-defined
    // extensions to carry information not related to the products
    // release level.
    protected String parseSRVRLSLV() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SRVRLSLV);
        return readString();
    }

    // Manager-Level Number Attribute Binary Integer Number specifies
    // the level of a defined DDM manager.
    protected int parseMGRLVLN() throws DisconnectException {
        return readUnsignedShort();
    }

    // Security Mechanims.
    protected int[] parseSECMEC() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SECMEC);
        return readUnsignedShortList();
    }

    // The Security Token Byte String is information provided and used
    // by the various security mechanisms.
    protected byte[] parseSECTKN(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SECTKN);
        if (skip) {
            skipBytes();
            return null;
        }
        return readBytes();
    }


    // The Security Check Code String codifies the security information
    // and condition for the SECCHKRM.
    protected int parseSECCHKCD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SECCHKCD);
        int secchkcd = readUnsignedByte();
        if ((secchkcd < CodePoint.SECCHKCD_00) || (secchkcd > CodePoint.SECCHKCD_15)) {
            doValnsprmSemantics(CodePoint.SECCHKCD, secchkcd);
        }
        return secchkcd;
    }

    // Product specific Identifier specifies the product release level
    // of a DDM server.
    protected String parsePRDID(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.PRDID);
        if (skip) {
            skipBytes();
            return null;
        } else {
            return readString();
        }
    }

    // The User Id specifies an end-user name.
    protected String parseUSRID(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.USRID);
        if (skip) {
            skipBytes();
            return null;
        }
        return readString();
    };

    // Code Point Data Representation specifies the data representation
    // of a dictionary codepoint.  Code points are hexadecimal aliases for DDM
    // named terms.
    protected int parseCODPNTDR() throws DisconnectException {
        return readUnsignedShort();
    }

    // Correlation Token specifies a token that is conveyed between source
    // and target servers for correlating the processing between servers.
    protected byte[] parseCRRTKN(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.CRRTKN);
        if (skip) {
            skipBytes();
            return null;
        }
        return readBytes();
    }

    // Unit of Work Disposition Scalar Object specifies the disposition of the
    // last unit of work.
    protected int parseUOWDSP() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.UOWDSP);
        int uowdsp = readUnsignedByte();
        if ((uowdsp != CodePoint.UOWDSP_COMMIT) && (uowdsp != CodePoint.UOWDSP_ROLLBACK)) {
            doValnsprmSemantics(CodePoint.UOWDSP, uowdsp);
        }
        return uowdsp;
    }


    // Relational Database Name specifies the name of a relational
    // database of the server.  A server can have more than one RDB.
    protected String parseRDBNAM(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.RDBNAM);
        if (skip) {
            skipBytes();
            return null;
        }
        return readString();
    };



    protected int parseXIDCNT() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.XIDCNT);
        return readUnsignedShort();
    }

    protected Xid parseXID() throws DisconnectException {
        return null;
    }

    protected java.util.Hashtable parseIndoubtList() throws DisconnectException {
        return null;
    }


    // Syntax Error Code String specifies the condition that caused termination
    // of data stream parsing.
    protected int parseSYNERRCD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SYNERRCD);
        int synerrcd = readUnsignedByte();
        if ((synerrcd < 0x01) || (synerrcd > 0x1D)) {
            doValnsprmSemantics(CodePoint.SYNERRCD, synerrcd);
        }
        return synerrcd;
    }

    // The Code Point Data specifies a scalar value that is an architected code point.
    protected int parseCODPNT() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.CODPNT);
        return parseCODPNTDR();
    }

    // Conversational Protocol Error Code specifies the condition
    // for which the PRCCNVRm was returned.
    protected int parsePRCCNVCD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.PRCCNVCD);
        int prccnvcd = readUnsignedByte();
        if ((prccnvcd != 0x01) && (prccnvcd != 0x02) && (prccnvcd != 0x03) &&
                (prccnvcd != 0x04) && (prccnvcd != 0x05) && (prccnvcd != 0x06) &&
                (prccnvcd != 0x10) && (prccnvcd != 0x11) && (prccnvcd != 0x12) &&
                (prccnvcd != 0x13) && (prccnvcd != 0x15)) {
            doValnsprmSemantics(CodePoint.PRCCNVCD, prccnvcd);
        }
        return prccnvcd;
    }

    // CCSID for Single-Byte Characters specifies a coded character
    // set identifier for single-byte characters.
    protected int parseCCSIDSBC() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDSBC);
        return readUnsignedShort();
    }

    // CCSID for Mixed-Byte Characters specifies a coded character
    // set identifier for mixed-byte characters.
    protected int parseCCSIDMBC() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDMBC);
        return readUnsignedShort();
    }

    // CCSID for Double-Byte Characters specifies a coded character
    // set identifier for double-byte characters.
    protected int parseCCSIDDBC() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.CCSIDDBC);
        return readUnsignedShort();
    }

    // Severity Code is an indicator of the severity of a condition
    // detected during the execution of a command.
    protected int parseSVRCOD(int minSvrcod, int maxSvrcod) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SVRCOD);

        int svrcod = readUnsignedShort();
        if ((svrcod != CodePoint.SVRCOD_INFO) &&
                (svrcod != CodePoint.SVRCOD_WARNING) &&
                (svrcod != CodePoint.SVRCOD_ERROR) &&
                (svrcod != CodePoint.SVRCOD_SEVERE) &&
                (svrcod != CodePoint.SVRCOD_ACCDMG) &&
                (svrcod != CodePoint.SVRCOD_PRMDMG) &&
                (svrcod != CodePoint.SVRCOD_SESDMG)) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }

        if (svrcod < minSvrcod || svrcod > maxSvrcod) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }

        return svrcod;
    }

    protected int parseFastSVRCOD(int minSvrcod, int maxSvrcod) throws DisconnectException {
        matchCodePoint(CodePoint.SVRCOD);

        int svrcod = readFastUnsignedShort();
        if ((svrcod != CodePoint.SVRCOD_INFO) &&
                (svrcod != CodePoint.SVRCOD_WARNING) &&
                (svrcod != CodePoint.SVRCOD_ERROR) &&
                (svrcod != CodePoint.SVRCOD_SEVERE) &&
                (svrcod != CodePoint.SVRCOD_ACCDMG) &&
                (svrcod != CodePoint.SVRCOD_PRMDMG) &&
                (svrcod != CodePoint.SVRCOD_SESDMG)) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }

        if (svrcod < minSvrcod || svrcod > maxSvrcod) {
            doValnsprmSemantics(CodePoint.SVRCOD, svrcod);
        }

        return svrcod;
    }

    protected NetSqlca parseSQLCARD(Sqlca[] rowsetSqlca) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLCARD);
        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);
        NetSqlca netSqlca = parseSQLCARDrow(rowsetSqlca);
        adjustLengths(getDdmLength());
        return netSqlca;
    }
    //--------------------------parse FDOCA objects------------------------

    // SQLCARD : FDOCA EARLY ROW
    // SQL Communications Area Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLCAGRP; GROUP LID 0x54; ELEMENT TAKEN 0(all); REP FACTOR 1

    NetSqlca parseSQLCARDrow(Sqlca[] rowsetSqlca) throws DisconnectException {
        return parseSQLCAGRP(rowsetSqlca);
    }

    // SQLNUMROW : FDOCA EARLY ROW
    // SQL Number of Elements Row Description
    //
    // FORMAT FOR SQLAM LEVELS
    //   SQLNUMGRP; GROUP LID 0x58; ELEMENT TAKEN 0(all); REP FACTOR 1
    int parseSQLNUMROW() throws DisconnectException {
        return parseSQLNUMGRP();
    }

    int parseFastSQLNUMROW() throws DisconnectException {
        return parseFastSQLNUMGRP();
    }

    // SQLNUMGRP : FDOCA EARLY GROUP
    // SQL Number of Elements Group Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLNUM; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    private int parseSQLNUMGRP() throws DisconnectException {
        return readShort();
    }

    private int parseFastSQLNUMGRP() throws DisconnectException {
        return readFastShort();
    }

    // SQLCAGRP : FDOCA EARLY GROUP
    // SQL Communcations Area Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLSTATE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    //   SQLERRPROC; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    //   SQLCAXGRP; PROTOCOL TYPE N-GDA; ENVLID 0x52; Length Override 0
    //   SQLDIAGGRP; PROTOCOL TYPE N-GDA; ENVLID 0x56; Length Override 0
    private NetSqlca parseSQLCAGRP(Sqlca[] rowsetSqlca) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return null;
        }

        int sqlcode = readFastInt();
        byte[] sqlstate = readFastBytes(5);
        byte[] sqlerrproc = readFastBytes(8);
        NetSqlca netSqlca = new NetSqlca(netAgent_.netConnection_,
                sqlcode,
                sqlstate,
                sqlerrproc,
                netAgent_.targetTypdef_.getCcsidSbc());

        parseSQLCAXGRP(netSqlca);

        if (netAgent_.targetSqlam_ >= NetConfiguration.MGRLVL_7) {
            netSqlca.setRowsetRowCount(parseSQLDIAGGRP(rowsetSqlca));
        }

        return netSqlca;
    }

    // SQLCAXGRP : EARLY FDOCA GROUP
    // SQL Communications Area Exceptions Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLRDBNME; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 18
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLERRD1; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD2; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD3; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD4; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD5; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLWARN0; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN1; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN2; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN3; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN4; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN5; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN6; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN7; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN8; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARN9; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    //   SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLERRMSG_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 70
    //   SQLERRMSG_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 70
    private void parseSQLCAXGRP(NetSqlca netSqlca) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            netSqlca.setContainsSqlcax(false);
            return;
        }

        if (netAgent_.targetSqlam_ < NetConfiguration.MGRLVL_7) {
            // skip over the rdbnam for now
            //   SQLRDBNME; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 18
            skipFastBytes(18);
        }
        //   SQLERRD1 to SQLERRD6; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
        int[] sqlerrd = new int[6];
        readFastIntArray(sqlerrd);

        //   SQLWARN0 to SQLWARNA; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
        byte[] sqlwarn = readFastBytes(11);

        if (netAgent_.targetSqlam_ >= NetConfiguration.MGRLVL_7) {
            // skip over the rdbnam for now
            // SQLRDBNAME; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
            parseFastVCS();
        }


        int sqlerrmcCcsid = 0;
        byte[] sqlerrmc = readFastLDBytes();
        if (sqlerrmc != null) {
            sqlerrmcCcsid = netAgent_.targetTypdef_.getCcsidMbc();
            skipFastBytes(2);
        } else {
            sqlerrmc = readFastLDBytes();
            sqlerrmcCcsid = netAgent_.targetTypdef_.getCcsidSbc();
        }

        netSqlca.setSqlerrd(sqlerrd);
        netSqlca.setSqlwarnBytes(sqlwarn);
        netSqlca.setSqlerrmcBytes(sqlerrmc, sqlerrmcCcsid); // sqlerrmc may be null
    }

    // SQLDIAGGRP : FDOCA EARLY GROUP
    // SQL Diagnostics Group Description - Identity 0xD1
    // Nullable Group
    // SQLDIAGSTT; PROTOCOL TYPE N-GDA; ENVLID 0xD3; Length Override 0
    // SQLDIAGCN;  DRFA TYPE N-RLO; ENVLID 0xF6; Length Override 0
    // SQLDIAGCI;  PROTOCOL TYPE N-RLO; ENVLID 0xF5; Length Override 0
    private long parseSQLDIAGGRP(Sqlca[] rowsetSqlca) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return 0;
        }

        long row_count = parseSQLDIAGSTT(rowsetSqlca);
        parseSQLDIAGCI(rowsetSqlca);
        parseSQLDIAGCN();

        return row_count;
    }

    // this is duplicated in parseColumnMetaData, but different
    // DAGroup under NETColumnMetaData requires a lot more stuffs including
    // precsion, scale and other stuffs
    protected String parseFastVCS() throws DisconnectException {
        // doublecheck what readString() does if the length is 0
        return readFastString(readFastUnsignedShort(),
                netAgent_.targetTypdef_.getCcsidSbcEncoding());
    }
    //----------------------non-parsing computational helper methods--------------

    protected boolean checkAndGetReceivedFlag(boolean receivedFlag) throws DisconnectException {
        if (receivedFlag) {
            // this method will throw a disconnect exception if
            // the received flag is already true;
            doSyntaxrmSemantics(CodePoint.SYNERRCD_DUP_OBJ_PRESENT);
        }
        return true;
    }

    protected void checkRequiredObjects(boolean receivedFlag) throws DisconnectException {
        if (!receivedFlag) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }
    }

    protected void checkRequiredObjects(boolean receivedFlag,
                                        boolean receivedFlag2) throws DisconnectException {
        if (!receivedFlag || !receivedFlag2) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }
    }

    protected void checkRequiredObjects(boolean receivedFlag,
                                        boolean receivedFlag2,
                                        boolean receivedFlag3) throws DisconnectException {
        if (!receivedFlag || !receivedFlag2 || !receivedFlag3) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }
    }

    protected void checkRequiredObjects(boolean receivedFlag,
                                        boolean receivedFlag2,
                                        boolean receivedFlag3,
                                        boolean receivedFlag4) throws DisconnectException {
        if (!receivedFlag || !receivedFlag2 || !receivedFlag3 || !receivedFlag4) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }
    }

    protected void checkRequiredObjects(boolean receivedFlag,
                                        boolean receivedFlag2,
                                        boolean receivedFlag3,
                                        boolean receivedFlag4,
                                        boolean receivedFlag5,
                                        boolean receivedFlag6) throws DisconnectException {
        if (!receivedFlag || !receivedFlag2 || !receivedFlag3 || !receivedFlag4 ||
                !receivedFlag5 || !receivedFlag6) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }

    }

    protected void checkRequiredObjects(boolean receivedFlag,
                                        boolean receivedFlag2,
                                        boolean receivedFlag3,
                                        boolean receivedFlag4,
                                        boolean receivedFlag5,
                                        boolean receivedFlag6,
                                        boolean receivedFlag7) throws DisconnectException {
        if (!receivedFlag || !receivedFlag2 || !receivedFlag3 || !receivedFlag4 ||
                !receivedFlag5 || !receivedFlag6 || !receivedFlag7) {
            doSyntaxrmSemantics(CodePoint.SYNERRCD_REQ_OBJ_NOT_FOUND);
        }
    }

    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    protected void doObjnsprmSemantics(int codePoint) throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "The DDM object is not supported.  " +
                "Unsupported DDM object code point: 0x" + Integer.toHexString(codePoint),
                SqlState._58015));
    }

    // Also called by NetStatementReply.
    protected void doPrmnsprmSemantics(int codePoint) throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "The DDM parameter is not supported.  " +
                "Unsupported DDM parameter code point: 0x" + Integer.toHexString(codePoint),
                SqlState._58016));
    }

    // Also called by NetStatementReply
    void doValnsprmSemantics(int codePoint, int value) throws DisconnectException {
        doValnsprmSemantics(codePoint, Integer.toString(value));
    }

    void doValnsprmSemantics(int codePoint, String value) throws DisconnectException {

        // special case the FDODTA codepoint not to disconnect.
        if (codePoint == CodePoint.FDODTA) {
            agent_.accumulateReadException(new SqlException(agent_.logWriter_,
                    "The DDM parameter value is not supported.  " +
                    "DDM parameter code point having unsupported value : 0x" + Integer.toHexString(codePoint) +
                    ".  An input host variable may not be within the range the server supports.",
                    SqlState._58017));
            return;
        }

        if (codePoint == CodePoint.CCSIDSBC ||
                codePoint == CodePoint.CCSIDDBC ||
                codePoint == CodePoint.CCSIDMBC) {
            // the server didn't like one of the ccsids.
            // the message should reflect the error in question.  right now these values
            // will be hard coded but this won't be correct if our driver starts sending
            // other values to the server.  In order to pick up the correct values,
            // a little reorganization may need to take place so that this code (or
            // whatever code sets the message) has access to the correct values.
            int cpValue = 0;
            switch (codePoint) {
            case CodePoint.CCSIDSBC:
                cpValue = netAgent_.typdef_.getCcsidSbc();
                break;
            case CodePoint.CCSIDDBC:
                cpValue = netAgent_.typdef_.getCcsidDbc();
                break;
            case CodePoint.CCSIDMBC:
                cpValue = netAgent_.typdef_.getCcsidSbc();
                break;
            default:
                // should never be in this default case...
                cpValue = 0;
                break;
            }
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    "There is no available conversion for the source code page, " + cpValue +
                    ", to the target code page, " + value + "."));
            //"57017", -332));
            return;
        }
        // the problem isn't with one of the ccsid values so...

        // Returning more information would
        // require rearranging this code a little.
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "The DDM parameter value is not supported.  " +
                "DDM parameter code point having unsupported value : 0x" + Integer.toHexString(codePoint),
                SqlState._58017));

    }

    void doDtamchrmSemantics() throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "A Data Descriptor Mismatch Error was detected.",
                SqlState._58009));
    }

    // Messages
    //  SQLSTATE : 58010
    //      Execution failed due to a distribution protocol error that
    //      will affect the successful execution of subsequent DDM commands
    //      or SQL statements.
    //  SQLCODE : -30021
    //       Execution failed because of a Distributed Protocol
    //       Error that will affect the successful execution of subsequent
    //       commands and SQL statements: Manager <manager> at Level
    //       <level> not supported.
    //
    //       A system erro occurred that prevented successful connection
    //       of the application to the remote database.  This message (SQLCODE)
    //       is producted for SQL CONNECT statement.
    private void doMgrlvlrmSemantics(String manager, String level) throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that will " +
                "affect the successful execution of subsequent DDM commands or SQL statements.  " +
                "A connection could not be established to the database because " +
                "manager " + manager + " at level " + level + " is not supported.",
                SqlState._58010));

    }

    private void doMgrlvlrmSemantics(int manager, int level) throws DisconnectException {
        doMgrlvlrmSemantics("0x" + Integer.toHexString(manager),
                "0x" + Integer.toHexString(level));
    }

    private void doMgrlvlrmSemantics(int[] nameList, int[] levelList) throws DisconnectException {
        StringBuffer managerNames = new StringBuffer(100);
        StringBuffer managerLevels = new StringBuffer(100);

        int count = nameList.length;
        for (int i = 0; i < count; i++) {
            managerNames.append("0x");
            managerNames.append(nameList[i]);
            managerLevels.append("0x");
            managerLevels.append(levelList[i]);
            if (i != (count - 1)) {
                managerNames.append(",");
                managerLevels.append(",");
            }
        }
        doMgrlvlrmSemantics(managerNames.toString(), managerLevels.toString());
    }

    // The client can detect that a conversational protocol error has occurred.
    // This can also be detected at the server in which case a PRCCNVRM is returned.
    // The Conversation Protocol Error Code, PRCCNVRM, describes the various errors.
    //
    // Note: Not all of these may be valid at the client.  See descriptions for
    // which ones make sense for client side errors/checks.
    // Conversation Error Code                  Description of Error
    // -----------------------                  --------------------
    // 0x01                                     RPYDSS received by target communications manager.
    // 0x02                                     Multiple DSSs sent without chaining or multiple
    //                                          DSS chains sent.
    // 0x03                                     OBJDSS sent when not allowed.
    // 0x04                                     Request correlation identifier of an RQSDSS
    //                                          is less than or equal to the previous
    //                                          RQSDSS's request correlatio identifier in the chain.
    // 0x05                                     Request correlation identifier of an OBJDSS
    //                                          does not equal the request correlation identifier
    //                                          of the preceding RQSDSS.
    // 0x06                                     EXCSAT was not the first command after the connection
    //                                          was established.
    // 0x10                                     ACCSEC or SECCHK command sent in wrong state.
    // 0x11                                     SYNCCTL or SYNCRSY command is used incorrectly.
    // 0x12                                     RDBNAM mismatch between ACCSEC, SECCHK, and ACCRDB.
    // 0x13                                     A command follows one that returned EXTDTAs as reply object.
    //
    // When the client detects these errors, it will be handled as if a PRCCNVRM is returned
    // from the server.  In this PRCCNVRM case, PROTOCOL architects an SQLSTATE of 58008 or 58009
    // depening of the SVRCOD.  In this case, a 58009 will always be returned.
    // Messages
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //      Some possible reason codes include:
    //      121C Indicates that the user is not authorized to perform the requested command.
    //      1232 The command could not be completed because of a permanent error.
    //          In most cases, the server will be in the process of an abend.
    //      220A The target server has received an invalid data description.
    //          If a user SQLDA is specified, ensure that the fields are
    //          initialized correctly. Also, ensure that the length does not
    //          exceed the maximum allowed length for the data type being used.
    //
    //      The command or statement cannot be processed.  The current
    //          transaction is rolled back and the application is disconnected
    //          from the remote database.
    protected void doPrccnvrmSemantics(int conversationProtocolErrorCode) throws DisconnectException {
        // we may need to map the conversation protocol error code, prccnvcd, to some kind
        // of reason code.  For now just return the prccnvcd as the reason code
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "A PROTOCOL Conversational Protocol Error was detected.  " +
                "Reason: 0x" + Integer.toHexString(conversationProtocolErrorCode),
                SqlState._58009));
    }

    // SQL Diagnostics Condition Token Array - Identity 0xF7
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLTOKROW; ROW LID 0xE7; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    void parseSQLDCTOKS() throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        for (int i = 0; i < num; i++) {
            parseSQLTOKROW();
        }
    }

    // SQL Diagnostics Condition Information Array - Identity 0xF5
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLDCIROW; ROW LID 0xE5; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private void parseSQLDIAGCI(Sqlca[] rowsetSqlca) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        if (num == 0) {
            resetRowsetSqlca(rowsetSqlca, 0);
        }

        // lastRow is the row number for the last row that had a non-null SQLCA.
        int lastRow = 1;
        for (int i = 0; i < num; i++) {
            lastRow = parseSQLDCROW(rowsetSqlca, lastRow);
        }
        resetRowsetSqlca(rowsetSqlca, lastRow + 1);
    }

    // SQL Diagnostics Connection Array - Identity 0xF6
    // SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    // SQLCNROW;  ROW LID 0xE6; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    private void parseSQLDIAGCN() throws DisconnectException {
        if (readUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        int num = parseFastSQLNUMROW();
        for (int i = 0; i < num; i++) {
            parseSQLCNROW();
        }
    }

    // SQL Diagnostics Connection Row - Identity 0xE6
    // SQLCNGRP; GROUP LID 0xD6; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLCNROW() throws DisconnectException {
        parseSQLCNGRP();
    }

    // SQL Diagnostics Condition Row - Identity 0xE5
    // SQLDCGRP; GROUP LID 0xD5; ELEMENT TAKEN 0(all); REP FACTOR 1
    private int parseSQLDCROW(Sqlca[] rowsetSqlca, int lastRow) throws DisconnectException {
        return parseSQLDCGRP(rowsetSqlca, lastRow);
    }

    // SQL Diagnostics Token Row - Identity 0xE7
    // SQLTOKGRP; GROUP LID 0xD7; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLTOKROW() throws DisconnectException {
        parseSQLTOKGRP();
    }

    // check on SQLTOKGRP format
    private void parseSQLTOKGRP() throws DisconnectException {
        skipFastNVCMorNVCS();
    }

    // SQL Diagnostics Statement Group Description - Identity 0xD3
    // Nullable Group
    // SQLDSFCOD; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSCOST; PROTOCOL TYPE I4; ENVLID 0X02; Length Override 4
    // SQLDSLROW; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSNPM; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSNRS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSRNS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSDCOD; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDSROWC; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSNROW; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSROWCS; PROTOCOL TYPE FD; ENVLID 0x0E; Length Override 31
    // SQLDSACON; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACRH; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACRS; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACSL; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACSE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSACTY; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSCERR; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLDSMORE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    private long parseSQLDIAGSTT(Sqlca[] rowsetSqlca) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return 0;
        }
        int sqldsFcod = readFastInt(); // FUNCTION_CODE
        int sqldsCost = readFastInt(); // COST_ESTIMATE
        int sqldsLrow = readFastInt(); // LAST_ROW

        skipFastBytes(16);

        long sqldsRowc = readFastLong(); // ROW_COUNT

        skipFastBytes(24);

        return sqldsRowc;
    }

    // SQL Diagnostics Connection Group Description - Identity 0xD6
    // Nullable
    //
    // SQLCNSTATE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLCNSTATUS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLCNATYPE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLCNETYPE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 1
    // SQLCNPRDID; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    // SQLCNRDB; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLCNCLASS; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLCNAUTHID; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    private void parseSQLCNGRP() throws DisconnectException {
        skipBytes(18);
        String sqlcnRDB = parseFastVCS();    // RDBNAM
        String sqlcnClass = parseFastVCS();  // CLASS_NAME
        String sqlcnAuthid = parseFastVCS(); // AUTHID
    }

    // SQL Diagnostics Condition Group Description
    //
    // SQLDCCODE; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCSTATE; PROTOCOL TYPE FCS; ENVLID Ox30; Lengeh Override 5
    // SQLDCREASON; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCLINEN; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCROWN; PROTOCOL TYPE FD; ENVLID 0x0E; Lengeh Override 31
    // SQLDCER01; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER02; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER03; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCER04; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPART; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCPPOP; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    // SQLDCMSGID; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 10
    // SQLDCMDE; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 8
    // SQLDCPMOD; PROTOCOL TYPE FCS; ENVLID 0x30; Length Override 5
    // SQLDCRDB; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    // SQLDCTOKS; PROTOCOL TYPE N-RLO; ENVLID 0xF7; Length Override 0
    // SQLDCMSG_m; PROTOCOL TYPE NVMC; ENVLID 0x3F; Length Override 32672
    // SQLDCMSG_S; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 32672
    // SQLDCCOLN_m; PROTOCOL TYPE NVCM ; ENVLID 0x3F; Length Override 255
    // SQLDCCOLN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCCURN_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCCURN_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCPNAM_m; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCPNAM_s; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXGRP; PROTOCOL TYPE N-GDA; ENVLID 0xD3; Length Override 1
    private int parseSQLDCGRP(Sqlca[] rowsetSqlca, int lastRow) throws DisconnectException {
        int sqldcCode = readFastInt(); // SQLCODE
        String sqldcState = readFastString(5, netAgent_.targetTypdef_.getCcsidSbcEncoding()); // SQLSTATE
        int sqldcReason = readFastInt();  // REASON_CODE
        int sqldcLinen = readFastInt(); // LINE_NUMBER
        int sqldcRown = (int) readFastLong(); // ROW_NUMBER

        // save +20237 in the 0th entry of the rowsetSqlca's.
        // this info is going to be used when a subsequent fetch prior is issued, and if already
        // received a +20237 then we've gone beyond the first row and there is no need to
        // flow another fetch to the server.
        if (sqldcCode == 20237) {
            rowsetSqlca[0] = new NetSqlca(netAgent_.netConnection_,
                    sqldcCode,
                    sqldcState.getBytes(),
                    null,
                    netAgent_.targetTypdef_.getCcsidSbc());
        } else {
            if (rowsetSqlca[sqldcRown] != null) {
                rowsetSqlca[sqldcRown].resetRowsetSqlca(netAgent_.netConnection_,
                        sqldcCode,
                        sqldcState.getBytes(),
                        null,
                        netAgent_.targetTypdef_.getCcsidSbc());
            } else {
                rowsetSqlca[sqldcRown] = new NetSqlca(netAgent_.netConnection_,
                        sqldcCode,
                        sqldcState.getBytes(),
                        null,
                        netAgent_.targetTypdef_.getCcsidSbc());
            }
        }

        // reset all entries between lastRow and sqldcRown to null
        for (int i = lastRow + 1; i < sqldcRown; i++) {
            rowsetSqlca[i] = null;
        }

        skipFastBytes(47);
        String sqldcRdb = parseFastVCS(); // RDBNAM
        // skip the tokens for now, since we already have the complete message.
        parseSQLDCTOKS(); // MESSAGE_TOKENS
        String sqldcMsg = parseFastNVCMorNVCS(); // MESSAGE_TEXT

        // skip the following for now.
        skipFastNVCMorNVCS();  // COLUMN_NAME
        skipFastNVCMorNVCS();  // PARAMETER_NAME
        skipFastNVCMorNVCS();  // EXTENDED_NAMES

        parseSQLDCXGRP(); // SQLDCXGRP
        return sqldcRown;
    }

    // SQL Diagnostics Extended Names Group Description - Identity 0xD5
    // Nullable
    //
    // SQLDCXRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTBLN_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXTBLN_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXCRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXCSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXCNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXCRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXCSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXCNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXRRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXRRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXRSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXRNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    //
    // SQLDCXTRDB_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTSCH_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTNAM_m ; PROTOCOL TYPE NVCM; ENVLID 0x3F; Length Override 255
    // SQLDCXTRDB_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXTSCH_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    // SQLDCXTNAM_s ; PROTOCOL TYPE NVCS; ENVLID 0x33; Length Override 255
    private void parseSQLDCXGRP() throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }
        skipFastNVCMorNVCS();  // OBJECT_RDBNAM
        skipFastNVCMorNVCS();  // OBJECT_SCHEMA
        skipFastNVCMorNVCS();  // SPECIFIC_NAME
        skipFastNVCMorNVCS();  // TABLE_NAME
        String sqldcxCrdb = parseFastVCS();        // CONSTRAINT_RDBNAM
        skipFastNVCMorNVCS();  // CONSTRAINT_SCHEMA
        skipFastNVCMorNVCS();  // CONSTRAINT_NAME
        parseFastVCS();        // ROUTINE_RDBNAM
        skipFastNVCMorNVCS();  // ROUTINE_SCHEMA
        skipFastNVCMorNVCS();  // ROUTINE_NAME
        parseFastVCS();        // TRIGGER_RDBNAM
        skipFastNVCMorNVCS();  // TRIGGER_SCHEMA
        skipFastNVCMorNVCS();  // TRIGGER_NAME
    }

    private String parseFastNVCMorNVCS() throws DisconnectException {
        String stringToBeSet = null;
        int vcm_length = 0;
        int vcs_length = 0;
        if (readFastUnsignedByte() != CodePoint.NULLDATA) {
            vcm_length = readFastUnsignedShort();
            if (vcm_length > 0) {
                stringToBeSet = readFastString(vcm_length, netAgent_.targetTypdef_.getCcsidMbcEncoding());
            }
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                        "only one of NVCM, NVCS can be non-none"));
            }
        } else {
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                vcs_length = readFastUnsignedShort();
                if (vcs_length > 0) {
                    stringToBeSet = readFastString(vcs_length, netAgent_.targetTypdef_.getCcsidSbcEncoding());
                }
            }
        }
        return stringToBeSet;
    }

    private void skipFastNVCMorNVCS() throws DisconnectException {
        int vcm_length = 0;
        int vcs_length = 0;
        if (readFastUnsignedByte() != CodePoint.NULLDATA) {
            vcm_length = readFastUnsignedShort();
            if (vcm_length > 0)
            //stringToBeSet = readString (vcm_length, netAgent_.targetTypdef_.getCcsidMbcEncoding());
            {
                skipFastBytes(vcm_length);
            }
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                        "only one of NVCM, NVCS can be non-none"));
            }
        } else {
            if (readFastUnsignedByte() != CodePoint.NULLDATA) {
                vcs_length = readFastUnsignedShort();
                if (vcs_length > 0)
                //stringToBeSet = readString (vcs_length, netAgent_.targetTypdef_.getCcsidSbcEncoding());
                {
                    skipFastBytes(vcs_length);
                }
            }
        }
    }

    void resetRowsetSqlca(Sqlca[] rowsetSqlca, int row) {
        // rowsetSqlca can be null.
        int count = ((rowsetSqlca == null) ? 0 : rowsetSqlca.length);
        for (int i = row; i < count; i++) {
            rowsetSqlca[i] = null;
        }
    }
}



