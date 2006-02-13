/*

   Derby - Class org.apache.derby.client.net.NetStatementReply

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

import org.apache.derby.client.am.ColumnMetaData;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.PreparedStatementCallbackInterface;
import org.apache.derby.client.am.ResultSetCallbackInterface;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlState;
import org.apache.derby.client.am.Statement;
import org.apache.derby.client.am.StatementCallbackInterface;
import org.apache.derby.client.am.Types;
import org.apache.derby.client.am.Utils;


public class NetStatementReply extends NetPackageReply implements StatementReplyInterface {
    NetStatementReply(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    //----------------------------- entry points ---------------------------------

    public void readPrepareDescribeOutput(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parsePRPSQLSTTreply(statement);
        endOfSameIdChainData();
    }

    public void readExecuteImmediate(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parseEXCSQLIMMreply(statement);
        endOfSameIdChainData();
    }

    public void readOpenQuery(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parseOPNQRYreply(statement);
        endOfSameIdChainData();
    }

    public void readExecute(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException {
        startSameIdChainParse();
        parseEXCSQLSTTreply(preparedStatement);
        endOfSameIdChainData();
    }

    public void readPrepare(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parsePRPSQLSTTreply(statement);
        endOfSameIdChainData();
    }

    public void readDescribeInput(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException {
        if (longBufferForDecryption_ != null) {
            buffer_ = longBufferForDecryption_;
            pos_ = longPosForDecryption_;
            count_ = longCountForDecryption_;
            if (longBufferForDecryption_ != null && count_ > longBufferForDecryption_.length) {
                count_ = longBufferForDecryption_.length;
            }
            dssLength_ = 0;
            longBufferForDecryption_ = null;
        }

        startSameIdChainParse();
        parseDSCSQLSTTreply(preparedStatement, 1); // anything other than 0 for input
        endOfSameIdChainData();
    }

    public void readDescribeOutput(PreparedStatementCallbackInterface preparedStatement) throws DisconnectException {
        startSameIdChainParse();
        parseDSCSQLSTTreply(preparedStatement, 0);  // 0 for output
        endOfSameIdChainData();
    }

    public void readExecuteCall(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parseEXCSQLSTTreply(statement);
        endOfSameIdChainData();
    }


    //----------------------helper methods----------------------------------------

    //------------------parse reply for specific command--------------------------

    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    private void parsePRPSQLSTTreply(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP == CodePoint.SQLDARD) {
            // the sqlcagrp is most likely null for insert/update/deletes.  if it is null, then we can
            // peek ahead for the column number which most likely will be 0.  if it is 0, then we will
            // not new up a ColumnMetaData, and we can skip the rest of the bytes in sqldard.
            // if sqlcargrp is not null, (most likely for select's) then we will not peek ahead for the
            // column number since it will never be 0 in a select case.
            ColumnMetaData columnMetaData = null;
            NetSqlca netSqlca = null;
            boolean nullSqlca = peekForNullSqlcagrp();
            if (nullSqlca && peekNumOfColumns() == 0) {
                netSqlca = parseSQLDARD(columnMetaData, true); // true means to skip the rest of SQLDARD bytes
            } else {
                columnMetaData = new ColumnMetaData(netAgent_.logWriter_);
                netSqlca = parseSQLDARD(columnMetaData, false); // false means do not skip SQLDARD bytes.
            }

            statement.completePrepareDescribeOutput(columnMetaData,
                    netSqlca);
        } else if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);
            statement.completePrepare(netSqlca);
        } else {
            parsePrepareError(statement);
        }

    }

    // Parse the reply for the Describe SQL Statement Command.
    // This method handles the parsing of all command replies and reply data
    // for the dscsqlstt command.
    private void parseDSCSQLSTTreply(PreparedStatementCallbackInterface ps,
                                     int metaDataType) // 0 is output, else input
            throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP == CodePoint.SQLDARD) {
            ColumnMetaData columnMetaData = null;

            if (columnMetaData == null) {
                columnMetaData = new ColumnMetaData(netAgent_.logWriter_);
            }

            NetSqlca netSqlca = parseSQLDARD(columnMetaData, false);  // false means do not skip SQLDARD bytes
            if (columnMetaData.columns_ == 0) {
                columnMetaData = null;
            }

            if (metaDataType == 0) // DESCRIBE OUTPUT
            {
                ps.completeDescribeOutput(columnMetaData, netSqlca);
            } else {
                ps.completeDescribeInput(columnMetaData, netSqlca);
            }
        } else if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);
            if (metaDataType == 0) // DESCRIBE OUTPUT
            {
                ps.completeDescribeOutput(null, netSqlca);
            } else {
                ps.completeDescribeInput(null, netSqlca);
            }
        } else {
            parseDescribeError(ps);
        }

    }

    // Parse the reply for the Execute Immediate SQL Statement Command.
    // This method handles the parsing of all command replies and reply data
    // for the excsqlimm command.
    private void parseEXCSQLIMMreply(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP == CodePoint.RDBUPDRM) {
            parseRDBUPDRM();
            peekCP = parseTypdefsOrMgrlvlovrs();
        }

        switch (peekCP) {
        case CodePoint.ENDUOWRM:
            parseENDUOWRM(statement.getConnectionCallbackInterface());
            parseTypdefsOrMgrlvlovrs();
        case CodePoint.SQLCARD:
            NetSqlca netSqlca = parseSQLCARD(null);


            statement.completeExecuteImmediate(netSqlca);
            break;
        default:
            parseExecuteImmediateError(statement);
            break;
        }

    }

    // Parse the reply for the Open Query Command.
    // This method handles the parsing of all command replies and reply data for the opnqry command.
    // will be replaced by parseOPNQRYreply (see parseOPNQRYreplyProto)
    private void parseOPNQRYreply(StatementCallbackInterface statementI) throws DisconnectException {
        int peekCP = peekCodePoint();

        if (peekCP == CodePoint.OPNQRYRM) {
            parseOpenQuery(statementI);
            peekCP = peekCodePoint();
            if (peekCP == CodePoint.RDBUPDRM) {
                parseRDBUPDRM();
                peekCP = peekCodePoint();
            }
        } else if (peekCP == CodePoint.RDBUPDRM) {
            parseRDBUPDRM();
            parseOpenQuery(statementI);
            peekCP = peekCodePoint();
        } else if (peekCP == CodePoint.OPNQFLRM) {
            parseOpenQueryFailure(statementI);
            peekCP = peekCodePoint();
        } else {
            parseOpenQueryError(statementI);
            peekCP = peekCodePoint();
        }

    }

    // Called by NETSetClientPiggybackCommand.read()
    private void parseEXCSQLSETreply(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = parseTypdefsOrMgrlvlovrs();

        if (peekCP == CodePoint.RDBUPDRM) {
            parseRDBUPDRM();
            parseTypdefsOrMgrlvlovrs();
        } else if (peekCP == CodePoint.ENDUOWRM) {
            parseENDUOWRM(statement.getConnectionCallbackInterface());
            parseTypdefsOrMgrlvlovrs();
        }

        if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);
            statement.completeExecuteSetStatement(netSqlca);
        } else {
            parseExecuteSetStatementError(statement);
        }

    }

    // Parse the reply for the Execute SQL Statement Command.
    // This method handles the parsing of all command replies and reply data
    // for the excsqlstt command.
    // Also called by CallableStatement.readExecuteCall()
    private void parseEXCSQLSTTreply(StatementCallbackInterface statementI) throws DisconnectException {
        // first handle the transaction component, which consists of one or more
        // reply messages indicating the transaction state.
        // These are ENDUOWRM, CMMRQSRM, or RDBUPDRM.  If RDBUPDRM is returned,
        // it may be followed by ENDUOWRM or CMMRQSRM
        int peekCP = peekCodePoint();
        if (peekCP == CodePoint.RDBUPDRM) {
            parseRDBUPDRM();
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.ENDUOWRM) {
            parseENDUOWRM(statementI.getConnectionCallbackInterface());
            peekCP = peekCodePoint();
        }

        // Check for a RSLSETRM, this is first rm of the result set summary component
        // which would be returned if a stored procedure was called which returned result sets.
        if (peekCP == CodePoint.RSLSETRM) {
            parseResultSetProcedure(statementI);
            peekCP = peekCodePoint();
            if (peekCP == CodePoint.RDBUPDRM) {
                parseRDBUPDRM();
            }
            return;
        }

        // check for a possible TYPDEFNAM or TYPDEFOVR which may be present
        // before the SQLCARD or SQLDTARD.
        peekCP = parseTypdefsOrMgrlvlovrs();

        // an SQLCARD may be retunred if there was no output data, result sets or parameters,
        // or in the case of an error.
        if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);

            statementI.completeExecute(netSqlca);
        } else if (peekCP == CodePoint.SQLDTARD) {
            // in the case of singleton select or if a stored procedure was called which had
            // parameters but no result sets, an SQLSTARD may be returned
            // keep the PreparedStatementCallbackInterface, since only preparedstatement and callablestatement
            // has parameters or singleton select which translates to sqldtard.
            NetSqldta netSqldta = null;
            boolean useCachedSingletonRowData = false;
            if (((Statement) statementI).cachedSingletonRowData_ == null) {
                netSqldta = new NetSqldta(netAgent_);
            } else {
                netSqldta = (NetSqldta) ((Statement) statementI).cachedSingletonRowData_;
                netSqldta.resetDataBuffer();
                netSqldta.extdtaData_.clear();
                useCachedSingletonRowData = true;
            }
            NetSqlca netSqlca =
                    parseSQLDTARD(netSqldta);

            // there may be externalized LOB data which also gets returned.
            peekCP = peekCodePoint();
            while (peekCP == CodePoint.EXTDTA) {
                copyEXTDTA(netSqldta);
                peekCP = peekCodePoint();
            }
            statementI.completeExecuteCall(netSqlca, netSqldta);
        } else {
            // if here, then assume an error reply message was returned.
            parseExecuteError(statementI);
        }

    }

    protected void parseResultSetProcedure(StatementCallbackInterface statementI) throws DisconnectException {
        // when a stored procedure is called which returns result sets,
        // the next thing to be returned after the optional transaction component
        // is the summary component.
        //
        // Parse the Result Set Summary Component which consists of a
        // Result Set Reply Message, SQLCARD or SQLDTARD, and an SQL Result Set
        // Reply data object.  Also check for possible TYPDEF overrides before the
        // OBJDSSs.
        // This method returns an ArrayList of generated sections which contain the
        // package and section information for the result sets which were opened on the
        // server.

        // the result set summary component consists of a result set reply message.
        java.util.ArrayList sectionAL = parseRSLSETRM();

        // following the RSLSETRM is an SQLCARD or an SQLDTARD.  check for a
        // TYPDEFNAM or TYPDEFOVR before looking for these objects.
        int peekCP = parseTypdefsOrMgrlvlovrs();

        // The SQLCARD and the SQLDTARD are mutually exclusive.
        // The SQLDTARD is returned if the stored procedure had parameters.
        // (Note: the SQLDTARD contains an sqlca also.  this is the sqlca for the
        // stored procedure call.
        NetSqldta netSqldta = null;
        NetSqlca netSqlca = null;
        if (peekCP == CodePoint.SQLCARD) {
            netSqlca = parseSQLCARD(null);
        } else {
            // keep the PreparedStatementCallbackInterface, since only preparedstatement and callablestatement
            // has parameters or singleton select which translates to sqldtard.
            netSqldta = new NetSqldta(netAgent_);
            netSqlca = parseSQLDTARD(netSqldta);
        }

        // check for a possible TYPDEFNAM or TYPDEFOVR
        // before the SQL Result Set Reply Data object
        peekCP = parseTypdefsOrMgrlvlovrs();

        int numberOfResultSets = parseSQLRSLRD(sectionAL);

        // The result set summary component parsed above indicated how many result sets were opened
        // by the stored pocedure call.  It contained section information for
        // each of these result sets.  Loop through the section array and
        // parse the result set component for each of the retunred result sets.
        NetResultSet[] resultSets = new NetResultSet[numberOfResultSets];
        for (int i = 0; i < numberOfResultSets; i++) {
            // parse the result set component of the stored procedure reply.
            NetResultSet netResultSet = parseResultSetCursor(statementI, (Section) sectionAL.get(i));
            resultSets[i] = netResultSet;
        }

        // LOBs may have been returned for one of the stored procedure parameters so
        // check for any externalized data.
        peekCP = peekCodePoint();
        while (peekCP == CodePoint.EXTDTA) {
            copyEXTDTA(netSqldta);
            peekCP = peekCodePoint();
        }
        statementI.completeExecuteCall(netSqlca, netSqldta, resultSets);
    }

    // Parse the Result Set component of the reply for a stored procedure
    // call which returns result sets.
    // The Result Set component consists of an Open Query Reply Message
    // followed by an optional SQLCARD, followed by an optional
    // SQL Column Information Reply data object, followed by a Query Descriptor.
    // There may also be Query Data or an End of Query Reply Message.
    protected NetResultSet parseResultSetCursor(StatementCallbackInterface statementI,
                                                Section section) throws DisconnectException {
        // The first item returne is an OPNQRYRM.
        NetResultSet netResultSet = parseOPNQRYRM(statementI, false);

        // The next to be returned is an OBJDSS so check for any TYPDEF overrides.
        int peekCP = parseTypdefsOrMgrlvlovrs();

        // An SQLCARD may be returned if there were any warnings on the OPEN.
        NetSqlca netSqlca = null;
        if (peekCP == CodePoint.SQLCARD) {
            netSqlca = parseSQLCARD(null);
            peekCP = parseTypdefsOrMgrlvlovrs();
        }

        // the SQLCINRD contains SQLDA like information for the result set.
        ColumnMetaData resultSetMetaData = null;
        if (peekCP == CodePoint.SQLCINRD) {
            resultSetMetaData = parseSQLCINRD();
            peekCP = parseTypdefsOrMgrlvlovrs();
        }

        // A Query Descriptor must be present.
        // We cannot cache the cursor if result set is returned from a stored procedure, so
        // there is no cached cursor to use here.
        parseQRYDSC(netResultSet.netCursor_);
        peekCP = peekCodePoint();
        statementI.completeExecuteCallOpenQuery(netSqlca, netResultSet, resultSetMetaData, section);

        // Depending on the blocking rules, QRYDTA may have been returned on the open.
        while (peekCP == CodePoint.QRYDTA) {
            parseQRYDTA(netResultSet);
            peekCP = peekCodePoint();
        }

        // Under some circumstances, the server may have closed the cursor.
        // This will be indicated by an ENDQRYRM.
        if (peekCP == CodePoint.ENDQRYRM) {
            parseEndQuery((ResultSetCallbackInterface) netResultSet);
        }

        return netResultSet;
    }

    protected void parseOpenQuery(StatementCallbackInterface statementI) throws DisconnectException {
        NetResultSet netResultSet = parseOPNQRYRM(statementI, true);

        NetSqlca sqlca = null;
        int peekCP = peekCodePoint();
        if (peekCP != CodePoint.QRYDSC) {

            peekCP = parseTypdefsOrMgrlvlovrs();

            if (peekCP == CodePoint.SQLDARD) {
                ColumnMetaData columnMetaData = new ColumnMetaData(netAgent_.logWriter_);
                NetSqlca netSqlca = parseSQLDARD(columnMetaData, false);  // false means do not skip SQLDARD bytes

                //For java stored procedure, we got the resultSetMetaData from server,
                //Do we need to save the resultSetMetaData and propagate netSqlca?
                //The following statement are doing the both, but it do more than
                //we want. It also mark the completion of Prepare statement.
                //
                // this will override the same call made from parsePrepareDescribe
                //  this will not work, this is not the DA for the stored proc params
                statementI.completePrepareDescribeOutput(columnMetaData, netSqlca);
                peekCP = parseTypdefsOrMgrlvlovrs();
            }
            // check if the DARD is mutually exclusive with CARD, if so, then the following if should be an elese

            if (peekCP == CodePoint.SQLCARD) {
                sqlca = parseSQLCARD(null);
                peekCP = parseTypdefsOrMgrlvlovrs();
            }
        }
        parseQRYDSC(netResultSet.netCursor_);

        peekCP = peekCodePoint();
        while (peekCP == CodePoint.QRYDTA) {
            parseQRYDTA(netResultSet);
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.SQLCARD) {
            NetSqlca netSqlca = parseSQLCARD(null);
            statementI.completeSqlca(netSqlca);
            peekCP = peekCodePoint();
        }

        if (peekCP == CodePoint.ENDQRYRM) {
            parseEndQuery(netResultSet);
        }

        statementI.completeOpenQuery(sqlca, netResultSet);
    }

    protected void parseEndQuery(ResultSetCallbackInterface resultSetI) throws DisconnectException {
        parseENDQRYRM(resultSetI);
        parseTypdefsOrMgrlvlovrs();
        NetSqlca netSqlca = parseSQLCARD(null);
        resultSetI.earlyCloseComplete(netSqlca);
    }

    void parseOpenQueryFailure(StatementCallbackInterface statementI) throws DisconnectException {
        parseOPNQFLRM(statementI);
        parseTypdefsOrMgrlvlovrs();
        NetSqlca netSqlca = parseSQLCARD(null);
        statementI.completeOpenQuery(netSqlca, null);
    }

    void parsePrepareError(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statement.getConnectionCallbackInterface());
                statement.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.DTAMCHRM:
            parseDTAMCHRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        case CodePoint.SQLERRRM:
            {
                NetSqlca sqlca = parseSqlErrorCondition();
                statement.completeSqlca(sqlca);
                break;
            }
        default:
            parseCommonError(peekCP);
        }
    }

    void parseExecuteImmediateError(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statement.getConnectionCallbackInterface());
                statement.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.DTAMCHRM:
            parseDTAMCHRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        case CodePoint.SQLERRRM:
            {
                NetSqlca sqlca = parseSqlErrorCondition();
                statement.completeSqlca(sqlca);
                break;
            }
        default:
            parseCommonError(peekCP);
            break;
        }
    }


    void parseDescribeError(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statement.getConnectionCallbackInterface());
                statement.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        case CodePoint.SQLERRRM:
            {
                NetSqlca sqlca = parseSqlErrorCondition();
                statement.completeSqlca(sqlca);
                break;
            }
        default:
            parseCommonError(peekCP);
        }
    }


    void parseOpenQueryError(StatementCallbackInterface statementI) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statementI.getConnectionCallbackInterface());
                statementI.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.DTAMCHRM:
            parseDTAMCHRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.QRYPOPRM:
            parseQRYPOPRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        default:
            parseCommonError(peekCP);
        }
    }

    void parseExecuteError(StatementCallbackInterface statementI) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statementI.getConnectionCallbackInterface());
                statementI.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.DTAMCHRM:
            parseDTAMCHRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        case CodePoint.SQLERRRM:
            {
                NetSqlca sqlca = parseSqlErrorCondition();
                statementI.completeSqlca(sqlca);
                break;
            }
        default:
            parseCommonError(peekCP);
            break;
        }
    }

    void parseExecuteSetStatementError(StatementCallbackInterface statement) throws DisconnectException {
        int peekCP = peekCodePoint();
        switch (peekCP) {
        case CodePoint.ABNUOWRM:
            {
                NetSqlca sqlca = parseAbnormalEndUow(statement.getConnectionCallbackInterface());
                statement.completeSqlca(sqlca);
                break;
            }
        case CodePoint.CMDCHKRM:
            parseCMDCHKRM();
            break;
        case CodePoint.DTAMCHRM:
            parseDTAMCHRM();
            break;
        case CodePoint.OBJNSPRM:
            parseOBJNSPRM();
            break;
        case CodePoint.RDBNACRM:
            parseRDBNACRM();
            break;
        case CodePoint.SQLERRRM:
            {
                NetSqlca sqlca = parseSqlErrorCondition();
                statement.completeSqlca(sqlca);
                break;
            }
        default:
            parseCommonError(peekCP);
            break;
        }
    }


    //-----------------------------parse DDM Reply Messages-----------------------

    /**
     * Open Query Complete Reply Message indicates to the requester
     * that an OPNQRY or EXCSQLSTT command completed normally and that
     * the query process has been initiated.  It also indicates the
     * type of query protocol and cursor used for the query.
     * <p>
     * When an EXCSQLSTT contains an SQL statement that invokes a
     * stored procedure, and the procedure completes, an OPNQRYRM is
     * returned for each answer set.
     *
     * @param statementI statement callback interface
     * @param isOPNQRYreply If true, parse a reply to an OPNQRY
     * command. Otherwise, parse a reply to an EXCSQLSTT command.
     * @return a <code>NetResultSet</code> value
     * @exception DisconnectException
     */
    protected NetResultSet parseOPNQRYRM(StatementCallbackInterface statementI,
                                         boolean isOPNQRYreply)
        throws DisconnectException
    {
        // these need to be initialized to the correct default values.
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean svrcodReceived = false;
        int qryprctyp = 0;
        boolean qryprctypReceived = false;
        int sqlcsrhld = 0xF0;    // 0xF0 is false (default), 0xF1 is true.
        boolean sqlcsrhldReceived = false;
        int qryattscr = 0xF0;   // 0xF0 is false (default), 0xF1 is true.
        boolean qryattscrReceived = false;
        int qryattsns = CodePoint.QRYUNK;
        boolean qryattsnsReceived = false;
        int qryattupd = CodePoint.QRYUNK;
        boolean qryattupdReceived = false;
        long qryinsid = 0;
        boolean qryinsidReceived = false;


        int qryattset = 0xF0;    // 0xF0 is false (default), 0xF1 is true.
        boolean qryattsetReceived = false;

        parseLengthAndMatchCodePoint(CodePoint.OPNQRYRM);
        //pushLengthOnCollectionStack();
        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);
        int peekCP = peekCodePoint();
        int length = 0;

        //while (peekCP != Reply.END_OF_COLLECTION) {
        while (ddmLength > 0) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                length = peekedLength_;
                svrcod = parseFastSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_SESDMG);
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYPRCTYP) {
                foundInPass = true;
                qryprctypReceived = checkAndGetReceivedFlag(qryprctypReceived);
                length = peekedLength_;
                qryprctyp = parseFastQRYPRCTYP();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SQLCSRHLD) {
                // Indicates whether the requester specified the HOLD option.
                // When specified, the cursor is not closed upon execution of a commit operation.
                foundInPass = true;
                sqlcsrhldReceived = checkAndGetReceivedFlag(sqlcsrhldReceived);
                length = peekedLength_;
                sqlcsrhld = parseFastSQLCSRHLD();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYATTSCR) {
                foundInPass = true;
                qryattscrReceived = checkAndGetReceivedFlag(qryattscrReceived);
                length = peekedLength_;
                qryattscr = parseFastQRYATTSCR();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYATTSNS) {
                foundInPass = true;
                qryattsnsReceived = checkAndGetReceivedFlag(qryattsnsReceived);
                length = peekedLength_;
                qryattsns = parseFastQRYATTSNS();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYATTUPD) {
                foundInPass = true;
                qryattupdReceived = checkAndGetReceivedFlag(qryattupdReceived);
                length = peekedLength_;
                qryattupd = parseFastQRYATTUPD();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYINSID) {
                foundInPass = true;
                qryinsidReceived = checkAndGetReceivedFlag(qryinsidReceived);
                length = peekedLength_;
                qryinsid = parseFastQRYINSID();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.QRYATTSET) {
                foundInPass = true;
                qryattsetReceived = checkAndGetReceivedFlag(qryattsetReceived);
                length = peekedLength_;
                qryattset = parseFastQRYATTSET();
                ddmLength = adjustDdmLength(ddmLength, length);
                peekCP = peekCodePoint();
            }


            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        checkRequiredObjects(svrcodReceived, qryprctypReceived, qryinsidReceived);

        netAgent_.setSvrcod(svrcod);

        // hack for now until event methods are used below
        Statement statement = (Statement) statementI;

        // if there is a cached Cursor object, then use the cached cursor object.
        NetResultSet rs = null;
        if (statement.cachedCursor_ != null) {
            statement.cachedCursor_.resetDataBuffer();
            ((NetCursor) statement.cachedCursor_).extdtaData_.clear();
            rs = new NetResultSet(netAgent_,
                    (NetStatement) statement.materialStatement_,
                    statement.cachedCursor_,
                    //qryprctyp, //protocolType, CodePoint.FIXROWPRC | CodePoint.LMTBLKPRC
                    sqlcsrhld, //holdOption, 0xF0 for false (default) | 0xF1 for true.
                    qryattscr, //scrollOption, 0xF0 for false (default) | 0xF1 for true.
                    qryattsns, //sensitivity, CodePoint.QRYUNK | CodePoint.QRYINS
                    qryattset,
                    qryinsid, //instanceIdentifier, 0 (if not returned, check default) or number
                    calculateResultSetType(qryattscr, qryattsns, statement.resultSetType_),
                    calculateResultSetConcurrency(qryattupd, statement.resultSetConcurrency_),
                    calculateResultSetHoldability(sqlcsrhld));
        } else {
            rs = new NetResultSet(netAgent_,
                    (NetStatement) statement.materialStatement_,
                    new NetCursor(netAgent_, qryprctyp),
                    //qryprctyp, //protocolType, CodePoint.FIXROWPRC | CodePoint.LMTBLKPRC
                    sqlcsrhld, //holdOption, 0xF0 for false (default) | 0xF1 for true.
                    qryattscr, //scrollOption, 0xF0 for false (default) | 0xF1 for true.
                    qryattsns, //sensitivity, CodePoint.QRYUNK | CodePoint.QRYINS
                    qryattset,
                    qryinsid, //instanceIdentifier, 0 (if not returned, check default) or number
                    calculateResultSetType(qryattscr, qryattsns, statement.resultSetType_),
                    calculateResultSetConcurrency(qryattupd, statement.resultSetConcurrency_),
                    calculateResultSetHoldability(sqlcsrhld));
        }

        // QRYCLSIMP only applies to OPNQRY, not EXCSQLSTT
        final boolean qryclsimp =
            isOPNQRYreply &&
            (rs.resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY) &&
            netAgent_.netConnection_.serverSupportsQryclsimp();
        rs.netCursor_.setQryclsimpEnabled(qryclsimp);

        return rs;
    }


    // Also called by NetResultSetReply subclass.
    // The End of Query Reply Message indicates that the query process has
    // terminated in such a manner that the query or result set is now closed.
    // It cannot be resumed with the CNTQRY command or closed with the CLSQRY command.
    // The ENDQRYRM is always followed by an SQLCARD.
    protected void parseENDQRYRM(ResultSetCallbackInterface resultSetI) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.ENDQRYRM);
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
            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived);

        netAgent_.setSvrcod(svrcod);

    }


    // Query Previously Opened Reply Message is issued when an
    // OPNQRY command is issued for a query that is already open.
    // A previous OPNQRY command might have opened the query
    // which may not be closed.
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
    // PKGNAMCSN - required
    // SRVDGN - optional
    //
    private void parseQRYPOPRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;
        boolean pkgnamcsnReceived = false;
        Object pkgnamcsn = null;

        parseLengthAndMatchCodePoint(CodePoint.QRYPOPRM);
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
        checkRequiredObjects(svrcodReceived,
                rdbnamReceived,
                pkgnamcsnReceived);

        netAgent_.setSvrcod(svrcod);
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "An Open Query Command was issued for a query which was already open.",
                SqlState._58009));
    }

    // Open Query Failure (OPNQFLRM) Reply Message indicates that the
    // OPNQRY command failed to open the query.  The reason that the
    // target relational database was unable to open the query is reported in an
    // SQLCARD reply data object.
    // Whenever an OPNQFLRM is returned, an SQLCARD object must also be returned
    // following the OPNQFLRM.
    //
    // Returned from Server:
    //   SVRCOD - required (8 - ERROR)
    //   RDBNAM - required
    //   SRVDGN - optional
    private void parseOPNQFLRM(StatementCallbackInterface statement) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean rdbnamReceived = false;
        String rdbnam = null;

        parseLengthAndMatchCodePoint(CodePoint.OPNQFLRM);
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
                // skip the rdbnam since it doesn't tell us anything new.
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

        netAgent_.setSvrcod(svrcod);

        // get SQLSTATE from SQLCARD...
    }

    // RDB Result Set Reply Message (RSLSETRM) indicates that an
    // EXCSQLSTT command invoked a stored procedure, that the execution
    // of the stored procedure generated one or more result sets, and
    // additional information aobut these result sets follows the SQLCARD and
    // SQLDTARD in the reply data of the response
    //
    // Returned from Server:
    //   SVRCOD - required  (0 INFO)
    //   PKGSNLST - required
    //   SRVDGN - optional
    protected java.util.ArrayList parseRSLSETRM() throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        boolean pkgsnlstReceived = false;
        java.util.ArrayList pkgsnlst = null;

        parseLengthAndMatchCodePoint(CodePoint.RSLSETRM);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_INFO, CodePoint.SVRCOD_INFO);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.PKGSNLST) {
                // contain repeatable PKGNAMCSN
                foundInPass = true;
                pkgsnlstReceived = checkAndGetReceivedFlag(pkgsnlstReceived);
                pkgsnlst = parsePKGSNLST();
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(svrcodReceived, pkgsnlstReceived);

        netAgent_.setSvrcod(svrcod);

        return pkgsnlst;
    }

    //--------------------- parse DDM Reply Data--------------------------------------


    // SQL Data Reply Data consists of output data from the relational database (RDB)
    // processing of an SQL statement.  It also includes a description of the data.
    //
    // Returned from Server:
    //   FDODSC - required
    //   FDODTA - required
    protected NetSqlca parseSQLDTARD(NetSqldta netSqldta) throws DisconnectException {
        boolean fdodscReceived = false;
        boolean fdodtaReceived = false;

        parseLengthAndMatchCodePoint(CodePoint.SQLDTARD);
        pushLengthOnCollectionStack();

        NetSqlca netSqlca = null;
        int peekCP = peekCodePoint();
        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.FDODSC) {
                foundInPass = true;
                fdodscReceived = checkAndGetReceivedFlag(fdodscReceived);
                parseFDODSC(netSqldta);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.FDODTA) {
                foundInPass = true;
                fdodtaReceived = checkAndGetReceivedFlag(fdodtaReceived);
                netSqlca = parseFDODTA(netSqldta);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }

        }
        popCollectionStack();
        checkRequiredObjects(fdodscReceived, fdodtaReceived);
        netSqldta.calculateColumnOffsetsForRow();
        return netSqlca;
    }

    protected void parseQRYDSC(NetCursor cursor) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYDSC);
        parseSQLDTARDarray(cursor, false); // false means don't just skip the bytes
    }

    private void parseFDODSC(NetCursor cursor) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.FDODSC);
        parseSQLDTARDarray(cursor, false); // false means don't just skip the bytes
    }

    private void parseSQLDTARDarray(NetCursor cursor, boolean skipBytes) throws DisconnectException {
        if (skipBytes) {
            skipBytes();
        }
        int previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_START;
        int previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_START;
        int mddProtocolType = 0;
        int columnCount = 0;
        netAgent_.targetTypdef_.clearMddOverrides();

        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);

        while (ddmLength > 0) {

            int tripletLength = readFastUnsignedByte();
            int tripletType = readFastUnsignedByte();
            int tripletId = readFastUnsignedByte();

            switch (tripletType) {

            case FdocaConstants.MDD_TRIPLET_TYPE:
                if ((tripletLength != FdocaConstants.MDD_TRIPLET_SIZE) ||
                        (tripletId != FdocaConstants.NULL_LID)) {
                    descriptorErrorDetected();
                }
                checkPreviousSQLDTARDtriplet(previousTripletType,
                        FdocaConstants.SQLDTARD_TRIPLET_TYPE_MDD,
                        previousTripletId,
                        FdocaConstants.SQLDTARD_TRIPLET_ID_0);
                previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_MDD;
                previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_0;

                // read in remaining MDD bytes
                int mddClass = readFastUnsignedByte();
                int mddType = readFastUnsignedByte();
                int mddRefType = readFastUnsignedByte();
                mddProtocolType = readFastUnsignedByte();
                break;

            case FdocaConstants.NGDA_TRIPLET_TYPE: // rename to NGDA_TRIPLET_CODEPOINT
                if (tripletId != FdocaConstants.SQLDTAGRP_LID) {
                    descriptorErrorDetected();
                }
                checkPreviousSQLDTARDtriplet(previousTripletType,
                        FdocaConstants.SQLDTARD_TRIPLET_TYPE_GDA,
                        previousTripletId,
                        FdocaConstants.SQLDTARD_TRIPLET_ID_D0);
                previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_GDA;
                previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_0;

                // add a quick check to see if the table is altered (columns are added or deleted)
                // before reusing the cached cursor.  note: this check does not catch the case
                // where the number of columns stay the same, but the column type or length changes,
                // i.e. from integer to char.
                int columns = peekTotalColumnCount(tripletLength);
                // peek ahead to get the total number of columns.
                cursor.initializeColumnInfoArrays(netAgent_.targetTypdef_, columns, netAgent_.targetSqlam_);
                columnCount += parseSQLDTAGRPdataLabelsAndUpdateColumn(cursor, columnCount, tripletLength);
                break;


            case FdocaConstants.RLO_TRIPLET_TYPE:  // rename to RLO_TRIPLET_CODEPOINT

                switch (tripletId) {
                case FdocaConstants.SQLCADTA_LID:
                    if (tripletLength != FdocaConstants.SQLCADTA_RLO_SIZE) {
                        descriptorErrorDetected(); // DSCERRCD_06
                    }
                    checkPreviousSQLDTARDtriplet(previousTripletType,
                            FdocaConstants.SQLDTARD_TRIPLET_TYPE_RLO,
                            previousTripletId,
                            FdocaConstants.SQLDTARD_TRIPLET_ID_E0);
                    previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_RLO;
                    previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_E0;
                    checkFastRLO(FdocaConstants.RLO_SQLCADTA);
                    break;

                case FdocaConstants.SQLDTARD_LID:
                    if (tripletLength != FdocaConstants.SQLDTARD_RLO_SIZE) {
                        descriptorErrorDetected(); // DSCERRCD_06
                    }
                    checkPreviousSQLDTARDtriplet(previousTripletType,
                            FdocaConstants.SQLDTARD_TRIPLET_TYPE_RLO,
                            previousTripletId,
                            FdocaConstants.SQLDTARD_TRIPLET_ID_F0);
                    previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_RLO;
                    previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_F0;
                    checkFastRLO(FdocaConstants.RLO_SQLDTARD);
                    break;
                default:
                    descriptorErrorDetected(); // DSCERRCD_07
                    break;
                }
                break;

            case FdocaConstants.CPT_TRIPLET_TYPE:  // rename to CPT_TRIPLET_CODEPOINT
                if (tripletId != FdocaConstants.NULL_LID) {
                    descriptorErrorDetected();
                }
                checkPreviousSQLDTARDtriplet(previousTripletType,
                        FdocaConstants.SQLDTARD_TRIPLET_TYPE_CPT,
                        previousTripletId,
                        FdocaConstants.SQLDTARD_TRIPLET_ID_0);
                previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_CPT;
                previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_0;

                columnCount += parseSQLDTAGRPdataLabelsAndUpdateColumn(cursor, columnCount, tripletLength);
                break;


            case FdocaConstants.SDA_TRIPLET_TYPE:  // rename to SDA_TRIPLET_CODEPOINT
                if (tripletLength != FdocaConstants.SDA_TRIPLET_SIZE) {
                    descriptorErrorDetected();  // DSCERRCD_06
                }
                checkPreviousSQLDTARDtriplet(previousTripletType,
                        FdocaConstants.SQLDTARD_TRIPLET_TYPE_SDA,
                        previousTripletId,
                        FdocaConstants.SQLDTARD_TRIPLET_ID_SDA);
                previousTripletType = FdocaConstants.SQLDTARD_TRIPLET_TYPE_SDA;
                previousTripletId = FdocaConstants.SQLDTARD_TRIPLET_ID_SDA;
                netAgent_.targetTypdef_.setMddOverride(mddProtocolType, // mdd protocol type
                        tripletId, // fdocaTripletLid
                        readFastUnsignedByte(), // fdocaFieldType
                        readFastInt(), // ccsid
                        readFastUnsignedByte(), // characterSize
                        readFastUnsignedByte(), // mode
                        readFastShort());
                break;

            default:
                descriptorErrorDetected();  //DSCERRCD_01
                break;
            }

            ddmLength -= tripletLength;
        }

        adjustLengths(getDdmLength());

        // Allocate a char buffer after all of the descriptors have been parsed out.
        cursor.allocateCharBuffer();

        checkPreviousSQLDTARDtriplet(previousTripletType,
                FdocaConstants.SQLDTARD_TRIPLET_TYPE_END,
                previousTripletId,
                FdocaConstants.SQLDTARD_TRIPLET_ID_END);

    }

    private void checkPreviousSQLDTARDtriplet(int previousTripletType,
                                              int tripletType,
                                              int previousTripletId,
                                              int tripletId) throws DisconnectException {
        if (FdocaConstants.SQLDTARD_TRIPLET_TYPES[previousTripletType][tripletType] == false) {
            descriptorErrorDetected(); // DSCERRCD_02 move error identity into array
        }
        if (FdocaConstants.SQLDTARD_TRIPLET_IDS[previousTripletId][tripletId] == false) {
            descriptorErrorDetected(); // DSCERRCD_02 move error identity into array
        }
    }


    private void checkFastRLO(int[][] rlo) throws DisconnectException {
        for (int i = 0; i < rlo.length; i++) {
            int lid = readFastUnsignedByte();
            if (lid != rlo[i][FdocaConstants.RLO_GROUP_LID]) {
                descriptorErrorDetected(); // DSCERRCD_42
            }
            int elementTaken = readFastUnsignedByte();
            if (elementTaken != rlo[i][FdocaConstants.RLO_ELEMENT_TAKEN]) {
                descriptorErrorDetected();  // DSCERRCD_07
            }
            int repFactor = readFastUnsignedByte();
            if (repFactor != rlo[i][FdocaConstants.RLO_REP_FACTOR]) {
                descriptorErrorDetected();  // DSCERRCD_07
            }
        }
    }

    // Possible errors to detect include:
    // DSCERRCD_01 - FDOCA triplet is not used in PROTOCOL descriptors or type code is invalid
    // DSCERRCD_02 - FDOCA triplet sequence error
    // DSCERRCD_03 - An array description is required and this is not one
    //               (too many or too few RLO triplets)
    // DSCERRCD_04 - A row description is required and this is not one
    //               (too many or too few RLO triplets)
    // DSCERRCD_05 - Late Environmental Descriptor just received not supported
    // DSCERRCD_06 - Malformed triplet, required parameter is missing
    // DSCERRCD_07 - Parameter value is not acceptable
    // DSCERRCD_11 - MDD present is not recognized as an SQL descriptor
    // DSCERRCD_12 - MDD class is not recognized as a valid SQL class
    // DSCERRCD_13 - MDD type not recognized as a valid SQL type
    // DSCERRCD_21 - Representation is incompatible with SQL type (in prior MDD)
    // DSCERRCD_22 - CCSID is not supported
    // DSCERRCD_32 - GDA references a local identifier which is not an SDA or GDA
    // DSCERRCD_33 - GDA length override exceeds limits
    // DSCERRCD_34 - GDA precision exceeds limits
    // DSCERRCD_35 - GDA scale greater than precision or scale negative
    // DSCERRCD_36 - GDA length override missing or incompatible with data type
    // DSCERRCD_41 - RLO references a LID which is not an RLO or GDA.
    // DSCERRCD_42 - RLO fails to reference a required GDA or RLO.
    private void descriptorErrorDetected() throws DisconnectException {
        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                "Execution failed due to a distribution protocol error that caused " +
                "deallocation of the conversation.  " +
                "A PROTOCOL Invalid FDOCA Description Error was detected.",
                SqlState._58009));
    }

    protected void parseQRYDTA(NetResultSet netResultSet) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYDTA);
        if (longValueForDecryption_ == null) {
            int ddmLength = getDdmLength();
            ensureBLayerDataInBuffer(ddmLength);
        }
        parseSQLDTARDdata(netResultSet.netCursor_);
        if (longValueForDecryption_ == null) {
            adjustLengths(getDdmLength());
        } else {
            longValueForDecryption_ = null;
        }
        if (longBufferForDecryption_ != null) {
            buffer_ = longBufferForDecryption_;
            pos_ = longPosForDecryption_;
            if (count_ > longBufferForDecryption_.length) {
                count_ = longBufferForDecryption_.length;
            } else if (longCountForDecryption_ != 0) {
                count_ = longCountForDecryption_;
                longCountForDecryption_ = 0;
            }
            dssLength_ = 0;
            longBufferForDecryption_ = null;
        }


    }

    NetSqlca parseFDODTA(NetCursor netCursor) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.FDODTA);
        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);
        mark();
        NetSqlca netSqlca = parseSQLCARDrow(null);
        int length = getFastSkipSQLCARDrowLength();
        adjustLengths(length);
        parseFastSQLDTARDdata(netCursor);
        return netSqlca;
    }

    void parseFastSQLDTARDdata(NetCursor netCursor) throws DisconnectException {
        netCursor.dataBufferStream_ = getFastData(netCursor.dataBufferStream_);
        netCursor.dataBuffer_ = netCursor.dataBufferStream_.toByteArray();
        netCursor.lastValidBytePosition_ = netCursor.dataBuffer_.length;
    }

    void parseSQLDTARDdata(NetCursor netCursor) throws DisconnectException {
        if (longValueForDecryption_ == null) {
            netCursor.dataBufferStream_ = getData(netCursor.dataBufferStream_);
            netCursor.dataBuffer_ = netCursor.dataBufferStream_.toByteArray();
        } else {
            int size = netCursor.dataBufferStream_.size();
            if (size == 0) {
                netCursor.dataBuffer_ = longValueForDecryption_;
                //longValue_ = null;
            } else {
                byte[] newArray = new byte[size + longValueForDecryption_.length];
                System.arraycopy(netCursor.dataBuffer_, 0, newArray, 0, size);
                System.arraycopy(longValueForDecryption_, 0, newArray, size, longValueForDecryption_.length);
                netCursor.dataBuffer_ = newArray;
                //longValue_ = null;
            }
        }

        netCursor.lastValidBytePosition_ = netCursor.dataBuffer_.length;
    }

    protected void copyEXTDTA(NetCursor netCursor) throws DisconnectException {
        try {
            parseLengthAndMatchCodePoint(CodePoint.EXTDTA);
            byte[] data = null;
            if (longValueForDecryption_ == null) {
                data = (getData(null)).toByteArray();
            } else {
                data = longValueForDecryption_;
                dssLength_ = 0;
                longValueForDecryption_ = null;
            }
            netCursor.extdtaData_.add(data);
        } catch (java.lang.OutOfMemoryError e) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    "Attempt to fully materialize lob data that is too large for the JVM.  "));
        }
    }

    //------------------------parse DDM Scalars-----------------------------

    // RDB Package name, consistency token, and section number
    // specifies the fully qualified name of a relational
    // database package, its consistency token, and a specific
    // section within a package.
    //
    // Only called for generated secctions from a callable statement.
    //
    protected Object parsePKGNAMCSN(boolean skip) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.PKGNAMCSN);
        if (skip) {
            skipBytes();
            return null;
        }

        // Still need to populate the logical members in case of an "set current packageset"
        String rdbnam = null;
        String rdbcolid = null;
        String pkgid = null;
        byte[] pkgcnstkn = null;

        int pkgsn = 0;
        byte[] pkgnamcsnBytes = null;
        int pkgnamcsnLength = 0;

        int ddmLength = getDdmLength();
        int offset = 0;

        ensureBLayerDataInBuffer(ddmLength);

        if (ddmLength == 64) {
            // read all the bytes except the section number into the byte[] for caching
            pkgnamcsnLength = ddmLength - 2;
            //pkgnamcsnBytes = readBytes (pkgnamcsnLength);
            pkgnamcsnBytes = new byte[pkgnamcsnLength];
            // readFast() does a read without moving the read head.
            offset = peekFastBytes(pkgnamcsnBytes, offset, pkgnamcsnLength);

            // populate the logical members
            rdbnam = readFastString(18);   // RDB name
            rdbcolid = readFastString(18); // RDB Collection ID
            pkgid = readFastString(18);    // RDB Package ID
            pkgcnstkn = readFastBytes(8);  // Package Consistency Token
        } else if ((ddmLength >= 71) && (ddmLength <= 781)) {
            // this is the new SCLDTA format.

            // new up a byte[] to cache all the bytes except the 2-byte section number
            pkgnamcsnBytes = new byte[ddmLength - 2];

            // get rdbnam
            int scldtaLen = peekFastLength();
            if (scldtaLen < 18 || scldtaLen > 255) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                        "scldta length, " + scldtaLen + ", is invalid for rdbnam"));
                return null;
            }
            // read 2+scldtaLen number of bytes from the reply buffer into the pkgnamcsnBytes
            //offset = readBytes (pkgnamcsnBytes, offset, 2+scldtaLen);
            offset = peekFastBytes(pkgnamcsnBytes, offset, 2 + scldtaLen);
            skipFastBytes(2);
            rdbnam = readFastString(scldtaLen);

            // get rdbcolid
            scldtaLen = peekFastLength();
            if (scldtaLen < 18 || scldtaLen > 255) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                        "scldta length, " + scldtaLen + ", is invalid for rdbcolid"));
                return null;
            }
            // read 2+scldtaLen number of bytes from the reply buffer into the pkgnamcsnBytes
            offset = peekFastBytes(pkgnamcsnBytes, offset, 2 + scldtaLen);
            skipFastBytes(2);
            rdbcolid = readFastString(scldtaLen);

            // get pkgid
            scldtaLen = peekFastLength();
            if (scldtaLen < 18 || scldtaLen > 255) {
                agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                        "scldta length, " + scldtaLen + ", is invalid for pkgid"));
                return null; // To make compiler happy.
            }
            // read 2+scldtaLen number of bytes from the reply buffer into the pkgnamcsnBytes
            offset = peekFastBytes(pkgnamcsnBytes, offset, 2 + scldtaLen);
            skipFastBytes(2);
            pkgid = readFastString(scldtaLen);

            // get consistency token
            offset = peekFastBytes(pkgnamcsnBytes, offset, 8);
            pkgcnstkn = readFastBytes(8);

        } else {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    "PKGNAMCSN length, " + ddmLength + ", is invalid at SQLAM " + netAgent_.targetSqlam_));
            return null;  // To make compiler happy.
        }

        pkgsn = readFastUnsignedShort();  // Package Section Number.
        adjustLengths(ddmLength);
        // this is a server generated section
        // the -1 is set for holdability and it is not used for generated sections
        Section section = new Section(this.agent_, pkgid, pkgsn, null, -1, true);
        section.setPKGNAMCBytes(pkgnamcsnBytes);
        return section;
    }

    // Query Protocol type specifies the type of query protocol
    // the target SQLAM uses.
    protected int parseQRYPRCTYP() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYPRCTYP);
        int qryprctyp = parseCODPNTDR();
        if ((qryprctyp != CodePoint.FIXROWPRC) && (qryprctyp != CodePoint.LMTBLKPRC)) {
            doValnsprmSemantics(CodePoint.QRYPRCTYP, qryprctyp);
        }
        return qryprctyp;
    }

    protected int parseFastQRYPRCTYP() throws DisconnectException {
        matchCodePoint(CodePoint.QRYPRCTYP);
        int qryprctyp = readFastUnsignedShort();
        if ((qryprctyp != CodePoint.FIXROWPRC) && (qryprctyp != CodePoint.LMTBLKPRC)) {
            doValnsprmSemantics(CodePoint.QRYPRCTYP, qryprctyp);
        }
        return qryprctyp;
    }

    // hold cursor position state indicates whether the requester specified
    // the HOLD option on the SQL DECLARE CURSOR statement.  When the HOLD
    // option is specified, the cursor is not closed upon execution of a
    // commit operation.
    // The value TRUE indicates that the requester specifies the HOLD
    // operation.  The value FALSSE indicates that the requeter is not
    // specifying the HOLD option.
    protected int parseSQLCSRHLD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLCSRHLD);
        int sqlcsrhld = readUnsignedByte();
        // 0xF0 is false (default), 0xF1 is true  // use constants in if
        if ((sqlcsrhld != 0xF0) && (sqlcsrhld != 0xF1)) {
            doValnsprmSemantics(CodePoint.SQLCSRHLD, sqlcsrhld);
        }
        return sqlcsrhld;
    }

    protected int parseFastSQLCSRHLD() throws DisconnectException {
        matchCodePoint(CodePoint.SQLCSRHLD);
        int sqlcsrhld = readFastUnsignedByte();
        // 0xF0 is false (default), 0xF1 is true  // use constants in if
        if ((sqlcsrhld != 0xF0) && (sqlcsrhld != 0xF1)) {
            doValnsprmSemantics(CodePoint.SQLCSRHLD, sqlcsrhld);
        }
        return sqlcsrhld;
    }

    // Query Attribute for Scrollability indicates whether
    // a cursor is scrollable or non-scrollable
    protected int parseQRYATTSCR() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYATTSCR);
        int qryattscr = readUnsignedByte();  // use constants in if
        if ((qryattscr != 0xF0) && (qryattscr != 0xF1)) {
            doValnsprmSemantics(CodePoint.QRYATTSCR, qryattscr);
        }
        return qryattscr;
    }

    protected int parseFastQRYATTSCR() throws DisconnectException {
        matchCodePoint(CodePoint.QRYATTSCR);
        int qryattscr = readFastUnsignedByte();  // use constants in if
        if ((qryattscr != 0xF0) && (qryattscr != 0xF1)) {
            doValnsprmSemantics(CodePoint.QRYATTSCR, qryattscr);
        }
        return qryattscr;
    }

    // enabled for rowset positioning.
    protected int parseQRYATTSET() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYATTSET);
        int qryattset = readUnsignedByte();  // use constants in if
        if ((qryattset != 0xF0) && (qryattset != 0xF1)) {
            doValnsprmSemantics(CodePoint.QRYATTSET, qryattset);
        }
        return qryattset;
    }

    protected int parseFastQRYATTSET() throws DisconnectException {
        matchCodePoint(CodePoint.QRYATTSET);
        int qryattset = readFastUnsignedByte();  // use constants in if
        if ((qryattset != 0xF0) && (qryattset != 0xF1)) {
            doValnsprmSemantics(CodePoint.QRYATTSET, qryattset);
        }
        return qryattset;
    }

    // Query attribute for Sensitivity indicats the sensitivity
    // of an opened cursor to changes made to the underlying
    // base table.
    protected int parseQRYATTSNS() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYATTSNS);
        int qryattsns = readUnsignedByte();
        switch (qryattsns) {
        case CodePoint.QRYUNK:
        case CodePoint.QRYINS:
            break;
        default:
            doValnsprmSemantics(CodePoint.QRYATTSNS, qryattsns);
            break;
        }
        return qryattsns;
    }

    protected int parseFastQRYATTSNS() throws DisconnectException {
        matchCodePoint(CodePoint.QRYATTSNS);
        int qryattsns = readFastUnsignedByte();
        switch (qryattsns) {
        case CodePoint.QRYUNK:
        case CodePoint.QRYINS:
            break;
        default:
            doValnsprmSemantics(CodePoint.QRYATTSNS, qryattsns);
            break;
        }
        return qryattsns;
    }

    // Query Attribute for Updatability indicates the updatability
    // of an opened cursor.
    protected int parseQRYATTUPD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.QRYATTUPD);
        int qryattupd = readUnsignedByte();
        switch (qryattupd) {
        case CodePoint.QRYUNK:
        case CodePoint.QRYRDO:
        case CodePoint.QRYUPD:
            break;
        default:
            doValnsprmSemantics(CodePoint.QRYATTUPD, qryattupd);
            break;
        }
        return qryattupd;
    }

    protected int parseFastQRYATTUPD() throws DisconnectException {
        matchCodePoint(CodePoint.QRYATTUPD);
        int qryattupd = readFastUnsignedByte();
        switch (qryattupd) {
        case CodePoint.QRYUNK:
        case CodePoint.QRYRDO:
        case CodePoint.QRYUPD:
            break;
        default:
            doValnsprmSemantics(CodePoint.QRYATTUPD, qryattupd);
            break;
        }
        return qryattupd;
    }


    private long parseFastQRYINSID() throws DisconnectException {
        matchCodePoint(CodePoint.QRYINSID);
        return readFastLong();
    }


    // RDB Package Namce, Consistency Token, and Section Number List
    // specifies a list of fully qualified names of specific sections
    // within one or more packages.
    protected java.util.ArrayList parsePKGSNLST() throws DisconnectException {
        Object pkgnamcsn = null;
        java.util.ArrayList pkgsnlst = new java.util.ArrayList(); // what default size should we use

        parseLengthAndMatchCodePoint(CodePoint.PKGSNLST);
        pushLengthOnCollectionStack();
        while (peekCodePoint() != Reply.END_OF_COLLECTION) {
            pkgnamcsn = parsePKGNAMCSN(false);
            pkgsnlst.add(pkgnamcsn);
        }
        popCollectionStack();
        return pkgsnlst;
    }

    protected NetSqlca parseSQLDARD(ColumnMetaData columnMetaData,
                                    boolean skipBytes) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLDARD);
        return parseSQLDARDarray(columnMetaData, skipBytes);
    }

    protected int parseSQLRSLRD(java.util.ArrayList sectionAL) throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLRSLRD);
        return parseSQLRSLRDarray(sectionAL);
    }

    protected ColumnMetaData parseSQLCINRD() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLCINRD);
        int ddmLength = getDdmLength();
        ensureBLayerDataInBuffer(ddmLength);
        ColumnMetaData cm = parseSQLCINRDarray();
        adjustLengths(getDdmLength());
        return cm;
    }


    //--------------------------parse FDOCA objects------------------------

    // SQLDARD : FDOCA EARLY ARRAY
    // SQL Descriptor Area Row Description with SQL Communications Area
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLDAROW; ROW LID 0x60; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLCARD; ROW LID 0x64; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLDHROW; ROW LID 0xE0; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLDAROW; ROW LID 0x60; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    NetSqlca parseSQLDARDarray(ColumnMetaData columnMetaData,
                               boolean skipBytes) throws DisconnectException {
        int ddmLength = 0;
        if (!ensuredLengthForDecryption_ && longValueForDecryption_ == null) {  //if ensuredLength = true, means we already ensured length in decryptData, so don't need to do it again
            ddmLength = getDdmLength();
            ensureBLayerDataInBuffer(ddmLength);

        }
        if (longValueForDecryption_ != null) {
            buffer_ = longValueForDecryption_;
            pos_ = 0;
            count_ = longValueForDecryption_.length;
            //dssLength_ = 0;
        }


        NetSqlca netSqlca = null;
        if (skipBytes) {
            mark();
            netSqlca = parseSQLCARDrow(null);
            skipFastBytes(ddmLength - getFastSkipSQLCARDrowLength());
            adjustLengths(getDdmLength());
            return netSqlca;
        } else {
            netSqlca = parseSQLCARDrow(null);
        }

        parseSQLDHROW(columnMetaData);

        int columns = parseSQLNUMROW();
        if (columns > columnMetaData.columns_)  // this will only be true when columnMetaData.columns_ = 0 under deferred prepare
        // under deferred prepare the CMD arrays are not allocated until now, no guesses were made
        {
            columnMetaData.initializeCache(columns);
        } else // column count was guessed, don't bother reallocating arrays, just truncate their lengths
        {
            columnMetaData.columns_ = columns;
        }

        // is this correct for 0 SQLNUMROW
        // does rest of code expect a null ColumnMetaData object
        // or does rest of code expect an non null object
        // with columns_ set to 0

        for (int i = 0; i < columnMetaData.columns_; i++) {
            parseSQLDAROW(columnMetaData, i);
        }

        if (longValueForDecryption_ == null) {
            adjustLengths(getDdmLength());
        } else {
            dssLength_ = 0;
            longValueForDecryption_ = null;
        }


        return netSqlca;
    }


    // SQLRSLRD : FDOCA EARLY ARRAY
    // Data Array of a Result Set
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLRSROW; ROW LID 0x6F; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    //
    // SQL Result Set Reply Data (SQLRSLRD) is a byte string that specifies
    // information about result sets returned as reply data in the response to
    // an EXCSQLSTT command that invokes a stored procedure
    int parseSQLRSLRDarray(java.util.ArrayList sectionAL) throws DisconnectException {
        int numOfResultSets = parseSQLNUMROW();
        for (int i = 0; i < numOfResultSets; i++) {
            parseSQLRSROW((Section) sectionAL.get(i));
        }
        return numOfResultSets;
    }

    // SQLCINRD : FDOCA EARLY ARRAY
    // SQL Result Set Column Array Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLCIROW; ROW LID 0x6B; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLDHROW; ROW LID 0xE0; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLNUMROW; ROW LID 0x68; ELEMENT TAKEN 0(all); REP FACTOR 1
    //   SQLDAROW; ROW LID 0x60; ELEMENT TAKEN 0(all); REP FACTOR 0(all)
    //
    // SQL Result Set Column Information Reply Data (SQLCINRD) is a byte string
    // that specifies information about columns for a result set returned as
    // reply data in the response to an EXCSQLSTT command that invodes a stored
    // procedure
    ColumnMetaData parseSQLCINRDarray() throws DisconnectException {
        ColumnMetaData columnMetaData = new ColumnMetaData(netAgent_.logWriter_);

        parseSQLDHROW(columnMetaData);

        // possibly change initializeCache to not new up arrays if
        // parseSQLNUMROW returns 0
        columnMetaData.initializeCache(parseFastSQLNUMROW());

        // is this correct for 0 SQLNUMROW,
        // does rest of code expect a null ColumnMetaData object
        // or does rest of code expect an non null object
        // with columns_ set to 0
        for (int i = 0; i < columnMetaData.columns_; i++) {
            parseSQLDAROW(columnMetaData, i);
        }

        return columnMetaData;
    }

    // SQLDAROW : FDOCA EARLY ROW
    // SQL Data Area Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLDAGRP; GROUP LID 0x50; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLDAROW(ColumnMetaData columnMetaData,
                               int columnNumber) throws DisconnectException {
        parseSQLDAGRP(columnMetaData, columnNumber);
    }

    // SQLDHROW : FDOCA EARLY ROW
    // SQL Descriptor Header Row Description
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLDHGRP;  GROUP LID 0xD0; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLDHROW(ColumnMetaData columnMetaData) throws DisconnectException {
        parseSQLDHGRP(columnMetaData);
    }

    // SQLRSROW : FDOCA EARLY ROW
    // SQL Row Description for One Result Set Row
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLRSGRP; GROUP LID 0x5F; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void parseSQLRSROW(Section section) throws DisconnectException {
        parseSQLRSGRP(section);
    }


    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // SQLDAGRP : EARLY FDOCA GROUP
    // SQL Data Area Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLPRECISION; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLSCALE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLLENGTH; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    //   SQLTYPE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLCCSID; PROTOCOL TYPE FB; ENVLID 0x26; Length Override 2
    //   SQLNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 30
    //   SQLNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 30
    //   SQLLABEL_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 30
    //   SQLLABEL_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 30
    //   SQLCOMMENTS_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 254
    //   SQLCOMMENTS_m; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 254
    //
    // FORMAT FOR SQLAM == 6
    //   SQLPRECISION; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLSCALE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLLENGTH; PROTOCOL TYPE I8; ENVLID 0x16; Length Override 8
    //   SQLTYPE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLCCSID; PROTOCOL TYPE FB; ENVLID 0x26; Length Override 2
    //   SQLNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 30
    //   SQLNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 30
    //   SQLLABEL_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 30
    //   SQLLABEL_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 30
    //   SQLCOMMENTS_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 254
    //   SQLCOMMENTS_m; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 254
    //   SQLUDTGRP; PROTOCOL TYPE N-GDA; ENVLID 0x51; Length Override 0
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLPRECISION; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLSCALE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLLENGTH; PROTOCOL TYPE I8; ENVLID 0x16; Length Override 8
    //   SQLTYPE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLCCSID; PROTOCOL TYPE FB; ENVLID 0x26; Length Override 2
    //   SQLDOPTGRP; PROTOCOL TYPE N-GDA; ENVLID 0xD2; Length Override 0
    private void parseSQLDAGRP(ColumnMetaData columnMetaData,
                               int columnNumber) throws DisconnectException {
        long columnLength = 0;

        // 2-byte precision
        int precision = readFastShort();

        // 2-byte scale
        int scale = readFastShort();

        // 8 byte sqllength
        columnLength = readFastLong();

        // create a set method after sqlType and ccsid is read
        // possibly have it set the nullable
        int sqlType = readFastShort();

        // 2-byte sqlccsid
        // (NOTE: SQLCCSID is always flown as BIG ENDIAN, not as data!)
        // The C-Common Client also does the following:
        // 1. Determine which type of code page is to be used for this variable:
        // 2. Map the CCSID to the correct codepage:
        // 3. "Split" the CCSID to see whether it is for SBCS or MBCS:
        int ccsid = readFastUnsignedShort();

        columnMetaData.sqlPrecision_[columnNumber] = precision;
        columnMetaData.sqlScale_[columnNumber] = scale;
        columnMetaData.sqlLength_[columnNumber] = columnLength;
        columnMetaData.sqlType_[columnNumber] = sqlType;
        // Set the nullables
        columnMetaData.nullable_[columnNumber] = Utils.isSqlTypeNullable(sqlType);
        columnMetaData.sqlCcsid_[columnNumber] = ccsid;
        columnMetaData.types_[columnNumber] =
                Types.mapDERBYTypeToDriverType(true, sqlType, columnLength, ccsid); // true means isDescribed
        parseSQLDOPTGRP(columnMetaData, columnNumber);
    }

    // SQLUDTGRP : EARLY FDOCA GROUP
    // SQL User-Defined Data Group Description
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLUDTXTYPE; PROTOCOL TYPE I4; ENVLID 0X02; Length Override 4
    //   SQLUDTRDB; PROTOCOL TYPE VCS; ENVLID 0X32; Length Override 255
    //   SQLUDTSCHEMA_m; PROTOCOL TYPE VCM; ENVLID 0X3E; Length Override 255
    //   SQLUDTSCHEMA_s; PROTOCOL TYPE VCS; ENVLID 0X32; Length Override 255
    //   SQLUDTNAME_m; PROTOCOL TYPE VCM; ENVLID 0X3E; Length Override 255
    //   SQLUDTNAME_s; PROTOCOL TYPE VCS; ENVLID 0X32; Length Override 255
    private void parseSQLUDTGRP(ColumnMetaData columnMetaData,
                                int columnNumber) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }

    }

    // SQLDOPTGRP : EARLY FDOCA GROUP
    // SQL Descriptor Optional Group Description
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLUNNAMED; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLLABEL_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLLABEL_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLCOMMENTS_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLCOMMENTS_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLUDTGRP; PROTOCOL TYPE N-GDA; ENVLID 0x5B; Length Override 0
    //   SQLDXGRP; PROTOCOL TYPE N-GDA; ENVLID 0xD4; Length Override 0
    private void parseSQLDOPTGRP(ColumnMetaData columnMetaData,
                                 int columnNumber) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }

        //   SQLUNNAMED; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqlunnamed = readFastShort();

        //   SQLNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String name = parseFastVCMorVCS();

        //   SQLLABEL_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLLABEL_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String label = parseFastVCMorVCS();

        //   SQLCOMMENTS_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLCOMMENTS_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String colComments = parseFastVCMorVCS();

        if (columnMetaData.sqlName_ == null) {
            columnMetaData.sqlName_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlLabel_ == null) {
            columnMetaData.sqlLabel_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlUnnamed_ == null) {
            columnMetaData.sqlUnnamed_ = new short[columnMetaData.columns_];
        }
        if (columnMetaData.sqlComment_ == null) {
            columnMetaData.sqlComment_ = new String[columnMetaData.columns_];
        }
        columnMetaData.sqlName_[columnNumber] = name;
        columnMetaData.sqlLabel_[columnNumber] = label;
        columnMetaData.sqlUnnamed_[columnNumber] = sqlunnamed;
        columnMetaData.sqlComment_[columnNumber] = colComments;

        // possibly move all the assignments into a single method on the columnMetaData object

        parseSQLUDTGRP(columnMetaData, columnNumber);
        parseSQLDXGRP(columnMetaData, columnNumber);
    }

    // SQLDXGRP : EARLY FDOCA GROUP
    // SQL Descriptor Extended Group Description
    //
    // FORMAT FOR SQLAM >=7
    //   SQLXKEYMEM; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLXUPDATEABLE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLXGENERATED; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLXPARMMODE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLXRDBNAM; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLXCORNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLXCORNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLXBASENAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLXBASENAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLXSCHEMA_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLXSCHEMA_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLXNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLXNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    private void parseSQLDXGRP(ColumnMetaData columnMetaData,
                               int column) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }


        //   SQLXKEYMEM; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqlxkeymem = readFastShort();

        //   SQLXUPDATEABLE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqlxupdateable = readFastShort();

        //   SQLXGENERATED; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqlxgenerated = readFastShort();

        //   SQLXPARMMODE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqlxparmmode = readFastShort();

        //   SQLXRDBNAM; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqlxrdbnam = parseFastVCS();

        //   SQLXCORNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXCORNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqlxcorname = parseFastVCMorVCS();

        //   SQLXBASENAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXBASENAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqlxbasename = parseFastVCMorVCS();

        //   SQLXSCHEMA_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXSCHEMA_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqlxschema = parseFastVCMorVCS();

        //   SQLXNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLXNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqlxname = parseFastVCMorVCS();

        if (columnMetaData.sqlxKeymem_ == null) {
            columnMetaData.sqlxKeymem_ = new short[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxGenerated_ == null) {
            columnMetaData.sqlxGenerated_ = new short[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxParmmode_ == null) {
            columnMetaData.sqlxParmmode_ = new short[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxCorname_ == null) {
            columnMetaData.sqlxCorname_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxName_ == null) {
            columnMetaData.sqlxName_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxBasename_ == null) {
            columnMetaData.sqlxBasename_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxUpdatable_ == null) {
            columnMetaData.sqlxUpdatable_ = new int[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxSchema_ == null) {
            columnMetaData.sqlxSchema_ = new String[columnMetaData.columns_];
        }
        if (columnMetaData.sqlxRdbnam_ == null) {
            columnMetaData.sqlxRdbnam_ = new String[columnMetaData.columns_];
        }

        columnMetaData.sqlxKeymem_[column] = sqlxkeymem;
        columnMetaData.sqlxGenerated_[column] = sqlxgenerated;
        columnMetaData.sqlxParmmode_[column] = sqlxparmmode;
        columnMetaData.sqlxCorname_[column] = sqlxcorname;
        columnMetaData.sqlxName_[column] = sqlxname;
        columnMetaData.sqlxBasename_[column] = sqlxbasename;
        columnMetaData.sqlxUpdatable_[column] = sqlxupdateable;
        columnMetaData.sqlxSchema_[column] = (sqlxschema == null) ? columnMetaData.sqldSchema_ : sqlxschema;
        columnMetaData.sqlxRdbnam_[column] = (sqlxrdbnam == null) ? columnMetaData.sqldRdbnam_ : sqlxrdbnam;
    }

    // SQLDHGRP : EARLY FDOCA GROUP
    // SQL Descriptor Header Group Description
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLDHOLD; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDRETURN; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDSCROLL; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDSENSITIVE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDFCODE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDKEYTYPE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
    //   SQLDRDBNAM; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLDSCHEMA_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLDSCHEMA_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    private void parseSQLDHGRP(ColumnMetaData columnMetaData) throws DisconnectException {
        if (readFastUnsignedByte() == CodePoint.NULLDATA) {
            return;
        }


        //   SQLDHOLD; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldhold = readFastShort();

        //   SQLDRETURN; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldreturn = readFastShort();

        //   SQLDSCROLL; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldscroll = readFastShort();

        //   SQLDSENSITIVE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldsensitive = readFastShort();

        //   SQLDFCODE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldfcode = readFastShort();

        //   SQLDKEYTYPE; PROTOCOL TYPE I2; ENVLID 0x04; Length Override 2
        short sqldkeytype = readFastShort();

        //   SQLDRDBNAM; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqldrdbnam = parseFastVCS();

        //   SQLDSCHEMA_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
        //   SQLDSCHEMA_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
        String sqldschema = parseFastVCMorVCS();

        columnMetaData.sqldHold_ = sqldhold;
        columnMetaData.sqldReturn_ = sqldreturn;
        columnMetaData.sqldScroll_ = sqldscroll;
        columnMetaData.sqldSensitive_ = sqldsensitive;
        columnMetaData.sqldFcode_ = sqldfcode;
        columnMetaData.sqldKeytype_ = sqldkeytype;
        columnMetaData.sqldRdbnam_ = sqldrdbnam;
        columnMetaData.sqldSchema_ = sqldschema;
    }

    // SQLRSGRP : EARLY FDOCA GROUP
    // SQL Result Set Group Description
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLRSLOCATOR; PROTOCOL TYPE RSL; ENVLID 0x14; Length Override 4
    //   SQLRSNAME_m; PROTOCOL TYPE VCM; ENVLID 0x3E; Length Override 255
    //   SQLRSNAME_s; PROTOCOL TYPE VCS; ENVLID 0x32; Length Override 255
    //   SQLRSNUMROWS; PROTOCOL TYPE I4; ENVLID 0x02; Length Override 4
    private void parseSQLRSGRP(Section section) throws DisconnectException {

        int rsLocator = readInt();
        String rsName = parseVCMorVCS();  // ignore length change bt SQLAM 6 and 7
        int rsNumRows = readInt();
        // currently rsLocator and rsNumRows are not being used.
        section.setCursorName(rsName);
    }


    // this is duplicated in parseColumnMetaData, but different
    // DAGroup under NETColumnMetaData requires a lot more stuffs including
    // precsion, scale and other stuffs
    private String parseFastVCMorVCS() throws DisconnectException {
        String stringToBeSet = null;

        int vcm_length = readFastUnsignedShort();
        if (vcm_length > 0) {
            stringToBeSet = readFastString(vcm_length, netAgent_.targetTypdef_.getCcsidMbcEncoding());
        }
        int vcs_length = readFastUnsignedShort();
        if (vcm_length > 0 && vcs_length > 0) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    "only one of the VCM, VCS length can be greater than 0"));
        } else if (vcs_length > 0) {
            stringToBeSet = readFastString(vcs_length, netAgent_.targetTypdef_.getCcsidSbcEncoding());
        }

        return stringToBeSet;
    }

    private String parseVCMorVCS() throws DisconnectException {
        String stringToBeSet = null;

        int vcm_length = readUnsignedShort();
        if (vcm_length > 0) {
            stringToBeSet = readString(vcm_length, netAgent_.targetTypdef_.getCcsidMbcEncoding());
        }
        int vcs_length = readUnsignedShort();
        if (vcm_length > 0 && vcs_length > 0) {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    "only one of the VCM, VCS length can be greater than 0"));
        } else if (vcs_length > 0) {
            stringToBeSet = readString(vcs_length, netAgent_.targetTypdef_.getCcsidSbcEncoding());
        }

        return stringToBeSet;
    }

    //----------------------non-parsing computational helper methods--------------
    private int calculateResultSetType(int qryattscr, int qryattsns, int defaultType) {
        // We are passing in defaultType "FOWARD_ONLY", in case desired type
        // cannot be obtained,we don't want to set the type to Statement's type,
        // but we will set it to the default.

        if (qryattscr == 0xF0) {
            return java.sql.ResultSet.TYPE_FORWARD_ONLY;
        }

        switch (qryattsns) {
        case CodePoint.QRYINS:
            return java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
        default:
            return defaultType;
        }
    }

    private int calculateResultSetConcurrency(int qryattupd, int defaultConcurrency) {
        // QRYATTUPD does come back for forward-only cursors if the desired concurrency cannot be
        // obtained, in which case we don't want to set the concurrency to the default, but
        // we want to set it to the actual concurrency.
        switch (qryattupd) {
        case CodePoint.QRYRDO:
            return java.sql.ResultSet.CONCUR_READ_ONLY;
        case CodePoint.QRYUPD:
            return java.sql.ResultSet.CONCUR_UPDATABLE;
        default:
            return defaultConcurrency;
        }
    }

    private int calculateResultSetHoldability(int sqlcsrhld) {
        if (sqlcsrhld == 0xF0) {
            return org.apache.derby.jdbc.ClientDataSource.CLOSE_CURSORS_AT_COMMIT;
        } else {
            return org.apache.derby.jdbc.ClientDataSource.HOLD_CURSORS_OVER_COMMIT;
        }
    }

    private int parseSQLDTAGRPdataLabelsAndUpdateColumn(NetCursor cursor, int columnIndex, int tripletLength)
            throws DisconnectException {
        int numColumns = (tripletLength - 3) / 3;
        for (int i = columnIndex; i < columnIndex + numColumns; i++) {
            cursor.qrydscTypdef_.updateColumn(cursor, i, readFastUnsignedByte(), readFastUnsignedShort());
        }
        return numColumns;
    }


    private String parseSQLSTT() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLSTT);
        return parseSQLSTTGRP();
    }

    private String parseSQLSTTGRP() throws DisconnectException {
        int mixedNullInd = readUnsignedByte();
        int singleNullInd = 0;
        String sqlsttString = null;
        int stringLength = 0;

        if (mixedNullInd == CodePoint.NULLDATA) {
            singleNullInd = readUnsignedByte();
            if (singleNullInd == CodePoint.NULLDATA) {
                // throw DTAMCHRM
                doDtamchrmSemantics();
            }
            // read 4-byte length
            stringLength = readInt();
            // read sqlstt string
            sqlsttString = readString(stringLength, netAgent_.targetTypdef_.getCcsidSbcEncoding());
        } else {
            // read 4-byte length
            stringLength = readInt();
            // read sqlstt string
            sqlsttString = readString(stringLength, netAgent_.targetTypdef_.getCcsidMbcEncoding());
            // read null indicator
            singleNullInd = readUnsignedByte();
        }
        return sqlsttString;
    }

    public void readSetSpecialRegister(StatementCallbackInterface statement) throws DisconnectException {
        startSameIdChainParse();
        parseEXCSQLSETreply(statement);
        endOfSameIdChainData();
    }


}




