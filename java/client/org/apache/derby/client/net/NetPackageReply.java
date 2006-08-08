/*

   Derby - Class org.apache.derby.client.net.NetPackageReply

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

import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.SQLState;

public class NetPackageReply extends NetConnectionReply {
    NetPackageReply(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }


    NetSqlca parseSqlErrorCondition() throws DisconnectException {
        parseSQLERRRM();
        parseTypdefsOrMgrlvlovrs();
        NetSqlca netSqlca = parseSQLCARD(null);
        return netSqlca;
    }


    // Also called by NetStatementReply
    void parseDTAMCHRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.DTAMCHRM);
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
        doDtamchrmSemantics();
    }

    // RDB Update Reply Message indicates that a DDM command resulted
    // in an update at the target relational database.  If a command
    // generated multiple reply messages including an RDBUPDRM, then
    // the RDBUPDRM must be the first reply message for the command.
    // For each target server, the RDBUPDRM  must be returned the first
    // time an update is made to the target RDB within a unit of work.
    // The target server may optionally return the RDBUPDRM after subsequent
    // updates within the UOW.  If multiple target RDBs are involved with
    // the current UOW and updates are made with any of them, then the RDBUPDRM
    // must be returned in response to the first update at each of them.
    protected void parseRDBUPDRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.RDBUPDRM);
        pushLengthOnCollectionStack();

        // in XA Global transaction we need to know if we have a read-only
        //  transaction, if we get a RDBUPDRM this is NOT a read-only transaction
        //  currently only XAConnections care about read-only transactions, if
        //  non-XA wants this information they will need to initialize the flag
        //  at start of UOW
        netAgent_.netConnection_.setReadOnlyTransactionFlag(false);

        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_INFO);
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

        // call an event to indicate the server has been updated
        netAgent_.setSvrcod(svrcod);

    }

    // SQL Error Condition Reply Message indicates that an SQL error
    // has occurred.  It may be sent even though no reply message
    // precedes the SQLCARD object that is the normal
    // response to a command when an exception occurs.
    // The SQLERRM is also used when a BNDSQLSTT command is terminated
    // by an INTRRDBRQS command.
    // This reply message must precede an SQLCARD object.
    // The SQLSTATE is returned in the SQLCARD.
    //
    // Returned from Server:
    // SVRCOD - required  (8 - ERROR)
    // RDBNAM - optional
    //
    // Also called by NetResultSetReply and NetStatementReply
    void parseSQLERRRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.SQLERRRM);
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
        checkRequiredObjects(svrcodReceived);

        // move into a method
        netAgent_.setSvrcod(svrcod);
    }

    //--------------------- parse DDM Reply Data--------------------------------------

    //------------------------parse DDM Scalars-----------------------------

    // RDB Package Name and Consistency token Scalar Object specifies the
    // fully qualified name of a relational database package and its
    // consistency token.
    protected Object parsePKGNAMCT(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.PKGNAMCT);
        if (skip) {
            skipBytes();
            return null;
        }
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                new ClientMessageId(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED),
                "parsePKGNAMCT"));
        return null; // to make compiler happy
    }
}
