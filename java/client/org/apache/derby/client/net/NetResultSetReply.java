/*

   Derby - Class org.apache.derby.client.net.NetResultSetReply

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

import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.ResultSet;
import org.apache.derby.client.am.ResultSetCallbackInterface;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;

public class NetResultSetReply extends NetStatementReply implements ResultSetReplyInterface {
    public NetResultSetReply(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    //----------------------------- entry points ---------------------------------

    public void readFetch(ResultSetCallbackInterface resultSet) throws DisconnectException {
        startSameIdChainParse();
        parseCNTQRYreply(resultSet, true); // true means we expect row data
        endOfSameIdChainData();
    }

    public void readPositioningFetch(ResultSetCallbackInterface resultSet) throws DisconnectException {
        startSameIdChainParse();
        parseCNTQRYreply(resultSet, false);  // false means return data is not expected
        endOfSameIdChainData();
    }

    public void readScrollableFetch(ResultSetCallbackInterface resultSet) throws DisconnectException {
        startSameIdChainParse();
        parseCNTQRYreply(resultSet, true);   // true means return data is expected
        endOfSameIdChainData();
    }

    public void readCursorClose(ResultSetCallbackInterface resultSet) throws DisconnectException {
        startSameIdChainParse();
        parseCLSQRYreply(resultSet);
        endOfSameIdChainData();
    }

    //----------------------helper methods----------------------------------------

    //------------------parse reply for specific command--------------------------

    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // Parse the reply for the Close Query Command.
    // This method handles the parsing of all command replies and reply data
    // for the clsqry command.
    private void parseCLSQRYreply(ResultSetCallbackInterface resultSet) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);  //@f48553sxg - null means rowsetSqlca_ is null
            // Set the cursor state if null SQLCA or sqlcode is equal to 0.
            resultSet.completeSqlca(netSqlca);
        } else {
            parseCloseError(resultSet);
        }
    }

    // Parse the reply for the Continue Query Command.
    // This method handles the parsing of all command replies and reply data for the cntqry command.
    // If doCopyQrydta==false, then there is no data, and we're only parsing out the sqlca to get the row count.
    private void parseCNTQRYreply(ResultSetCallbackInterface resultSetI,
                                  boolean doCopyQrydta) throws DisconnectException {
        boolean found = false;
        int peekCP = peekCodePoint();
        if (peekCP == CodePoint.RDBUPDRM) {
            found = true;
            parseRDBUPDRM();
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.QRYDTA) {
            found = true;
            if (!doCopyQrydta) {
                parseLengthAndMatchCodePoint(CodePoint.QRYDTA);
                //we don't need to copy QRYDTA since there is no data
                if (longValueForDecryption_ != null) {
                    longValueForDecryption_ = null;
                }
                if (longBufferForDecryption_ != null) {
                    longBufferForDecryption_ = null;
                }

                int ddmLength = getDdmLength();
                ensureBLayerDataInBuffer(ddmLength);
                ((ResultSet) resultSetI).expandRowsetSqlca();
                NetSqlca sqlca = parseSQLCARDrow(((ResultSet) resultSetI).rowsetSqlca_);
                int daNullIndicator = readFastByte();
                adjustLengths(getDdmLength());
                // define event interface and use the event method
                // only get the rowCount_ if sqlca is not null and rowCount_ is unknown
                if (sqlca != null && sqlca.containsSqlcax()) {
                    ((ResultSet) resultSetI).setRowCountEvent(sqlca.getRowCount(netAgent_.targetTypdef_));
                }

                peekCP = peekCodePoint();
                if (peekCP == CodePoint.RDBUPDRM) {
                    parseRDBUPDRM();
                    peekCP = peekCodePoint();
                }
                return;
            }
            do {
                parseQRYDTA((NetResultSet) resultSetI);
                peekCP = peekCodePoint();
            } while (peekCP == CodePoint.QRYDTA);
        }

        if (peekCP == CodePoint.EXTDTA) {
            found = true;
            do {
                copyEXTDTA((NetCursor) ((ResultSet) resultSetI).cursor_);
                if (longBufferForDecryption_ != null) {//encrypted EXTDTA
                    buffer_ = longBufferForDecryption_;
                    pos_ = longPosForDecryption_;
                    if (longBufferForDecryption_ != null && count_ > longBufferForDecryption_.length) {
                        count_ = longBufferForDecryption_.length;
                    }
                }

                peekCP = peekCodePoint();
            } while (peekCP == CodePoint.EXTDTA);
        }

        if (peekCP == CodePoint.SQLCARD) {
            found = true;
            ((ResultSet) resultSetI).expandRowsetSqlca();
            NetSqlca netSqlca = parseSQLCARD(((ResultSet) resultSetI).rowsetSqlca_);
            // for an atomic operation, the SQLCA contains the sqlcode for the first (statement
            // terminating)error, the last warning, or zero.  all multi-row fetch operatons are
            // atomic.  (the only operation that is not atomic is multi-row insert).
            if (((ResultSet) resultSetI).sensitivity_ != ResultSet.sensitivity_sensitive_dynamic__) {
                if (netSqlca != null && netSqlca.containsSqlcax() && netSqlca.getRowsetRowCount() == 0) {
                    ((ResultSet) resultSetI).setRowCountEvent(netSqlca.getRowCount(netAgent_.targetTypdef_));
                }
            }
            resultSetI.completeSqlca(netSqlca);
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.ENDQRYRM) {
            found = true;
            parseEndQuery(resultSetI);
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.RDBUPDRM) {
            found = true;
            parseRDBUPDRM();
        }

        if (!found) {
            parseFetchError(resultSetI);
        }

        if (longBufferForDecryption_ != null) {
            // Not a good idea to create a new buffer_
            buffer_ = new byte[DEFAULT_BUFFER_SIZE];
            longBufferForDecryption_ = null;
        }
    }

    void parseCloseError(ResultSetCallbackInterface resultSetI) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(resultSetI.getConnectionCallbackInterface());
                resultSetI.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.QRYNOPRM:
            parseQRYNOPRM(resultSetI);
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void parseFetchError(ResultSetCallbackInterface resultSetI) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(resultSetI.getConnectionCallbackInterface());
                resultSetI.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.CMDNSPRM:
            parseCMDNSPRM();
            break;
        case CodePoint.QRYNOPRM:
            parseQRYNOPRM(resultSetI);
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    //-----------------------------parse DDM Reply Messages-----------------------

    // Query Not Opened Reply Message is issued if a CNTQRY or CLSQRY
    // command is issued for a query that is not open.  A previous
    // ENDQRYRM, ENDUOWRM, or ABNUOWRM reply message might have
    // terminated the command.
    // PROTOCOL architects the SQLSTATE value depending on SVRCOD
    // SVRCOD 4 -> SQLSTATE is 24501
    // SVRCOD 8 -> SQLSTATE of 58008 or 58009
    //
    // if SVRCOD is 4 then SQLSTATE 24501, SQLCODE -501
    // else SQLSTATE 58009, SQLCODE -30020
    //
    // Messages
    // SQLSTATE : 24501
    //     The identified cursor is not open.
    //     SQLCODE : -501
    //     The cursor specified in a FETCH or CLOSE statement is not open.
    //     The statement cannot be processed.
    // SQLSTATE : 58009
    //     Execution failed due to a distribution protocol error that caused deallocation of the conversation.
    //     SQLCODE : -30020
    //     Execution failed because of a Distributed Protocol
    //         Error that will affect the successful execution of subsequent
    //         commands and SQL statements: Reason Code <reason-code>.
    //     Some possible reason codes include:
    //     121C Indicates that the user is not authorized to perform the requested command.
    //     1232 The command could not be completed because of a permanent error.
    //         In most cases, the server will be in the process of an abend.
    //     220A The target server has received an invalid data description.
    //         If a user SQLDA is specified, ensure that the fields are
    //         initialized correctly. Also, ensure that the length does not exceed
    //         the maximum allowed length for the data type being used.
    //
    //     The command or statement cannot be processed.  The current
    //         transaction is rolled back and the application is disconnected
    //         from the remote database.
    //
    // Returned from Server:
    // SVRCOD - required  (4 - WARNING, 8 - ERROR)
    // RDBNAM - required
    // PKGNAMCSN - required
    //
    private void parseQRYNOPRM(ResultSetCallbackInterface resultSet) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean pkgnamcsnReceived = false;
        Object pkgnamcsn = null;

        parseLengthAndMatchCodePoint(CodePoint.QRYNOPRM);
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

            if (peekCP == CodePoint.PKGNAMCSN) {
                foundInPass = true;
                pkgnamcsnReceived = checkAndGetReceivedFlag(pkgnamcsnReceived);
                pkgnamcsn = parsePKGNAMCSN(true);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, rdbnamReceived, pkgnamcsnReceived);

        // move into a method
        netAgent_.setSvrcod(svrcod);
        if (svrcod == CodePoint.SVRCOD_WARNING) {
            netAgent_.accumulateReadException(new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.DRDA_CURSOR_NOT_OPEN)));
        } else {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                new ClientMessageId(SQLState.DRDA_CONNECTION_TERMINATED),
                    SqlException.getMessageUtil().
                    getTextMessage(MessageId.CONN_CURSOR_NOT_OPEN)));
        }
    }
}

