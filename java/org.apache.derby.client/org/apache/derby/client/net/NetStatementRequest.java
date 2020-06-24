/*

   Derby - Class org.apache.derby.client.net.NetStatementRequest

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

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import org.apache.derby.client.am.ClientBlob;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.ClientClob;
import org.apache.derby.client.am.ColumnMetaData;
import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.DateTime;
import org.apache.derby.client.am.DateTimeValue;
import org.apache.derby.client.am.Lob;
import org.apache.derby.client.am.ClientResultSet;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientTypes;
import org.apache.derby.client.am.Utils;
import org.apache.derby.shared.common.reference.DRDAConstants;
import org.apache.derby.shared.common.reference.SQLState;

// For performance, should we worry about the ordering of our DDM command parameters

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class NetStatementRequest extends NetPackageRequest
    implements StatementRequestInterface {

    // Integers: build EXTDTA for column i
    private ArrayList<Integer> extdtaPositions_ = null;

    // promototed parameters hold parameters that are promotoed to a different
    // data type because they are too large to represent in PROTOCOL otherwise.
    // This currently only applies for promotion of (VAR)CHAR -> CLOB and (VAR)BINARY -> BLOB
    // The key for this structure is the parameter index.  Note that having this
    // collection does not eliminate the need for extdtaPositions_ because that
    // is still needed for non-promototed LOBs
    private final HashMap<Integer, Object> promototedParameters_ =
            new HashMap<Integer, Object>();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

//IC see: https://issues.apache.org/jira/browse/DERBY-728
//IC see: https://issues.apache.org/jira/browse/DERBY-4757
    NetStatementRequest(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    //----------------------------- entry points ---------------------------------

    // Write the message to perform an execute immediate.
    // The SQL statement sent as command data cannot contain references
    // to either input variables or output variables.
    //
    // preconditions:
    public void writeExecuteImmediate(NetStatement materialStatement,
                                      String sql,
                                      Section section) throws SqlException {
        buildEXCSQLIMM(section,
                false, //sendQryinsid
                0);                        //qryinsid
        buildSQLSTTcommandData(sql); // statement follows in sqlstt command data object
    }

    // Write the message to preform a prepare into.
    // Once the SQL statement has been prepared, it is executed until the unit of work, in
    // which the PRPSQLSTT command was issued, ends.  An exception to this is if
    // Keep Dynamic is being used.
    //
    // preconditions:
    public void writePrepareDescribeOutput(NetStatement materialStatement,
                                           String sql,
                                           Section section) throws SqlException {
        buildPRPSQLSTT(section,
                sql,
                true, //sendRtnsqlda
                true, //sendTypsqlda
                CodePoint.TYPSQLDA_X_OUTPUT);  //typsqlda

        if (((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_ != null) {
            buildSQLATTRcommandData(((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_);
        }

        buildSQLSTTcommandData(sql);  // statement follows in sqlstt command data object
    }

    // Write the message to perform a reprepare.
    //
    // preconditions:
    public void writePrepare(NetStatement materialStatement,
                             String sql,
                             Section section) throws SqlException {
        buildPRPSQLSTT(section,
                sql,
                false, //sendRtnsqlda
                false, //sendTypsqlda
                0);      //typsqlda

        if (((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_ != null) {
            buildSQLATTRcommandData(((NetStatement) materialStatement).statement_.cursorAttributesToSendOnPrepare_);
        }

        buildSQLSTTcommandData(sql); // statement follows in sqlstt command data object
    }

    // Write the message to execute  prepared sql statement.
    //
    // preconditions:
    public void writeExecute(NetPreparedStatement materialPreparedStatement,
                             Section section,
                             ColumnMetaData parameterMetaData,
                             Object[] inputs,
                             int numInputColumns,
                             boolean outputExpected,
                             boolean chained) throws SqlException  // chained flag for blobs only  //dupqry
    {
        buildEXCSQLSTT(section,
                true, // sendOutexp
                outputExpected, // outexp
                false, // sendPrcnam
                null, // prcnam
                false, // sendQryblksz
                false, // sendMaxrslcnt,
                0, // maxrslcnt,
                false, // sendMaxblkext
                0, // maxblkext
                false, // sendRslsetflg
                0, // resultSetFlag
                false, // sendQryrowset
                0);               // qryrowset

        if (numInputColumns > 0) {
            if ((extdtaPositions_ != null) && (!extdtaPositions_.isEmpty())) {
                extdtaPositions_.clear();  // reset extdta column position markers
            }

            boolean overrideExists = buildSQLDTAcommandData(numInputColumns,
                    parameterMetaData,
                    inputs);

            // can we eleminate the chain argument needed for lobs
            buildEXTDTA(parameterMetaData, inputs, chained);
        }
    }


    // Write the message to open a bound or prepared query with input parameters.
    // Check this -> For open query with input parameters
    //
    // preconditions:
    public void writeOpenQuery(NetPreparedStatement materialPreparedStatement,
                               Section section,
                               int fetchSize,
                               int resultSetType,
                               int numInputColumns,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                               ColumnMetaData parameterMetaData,
                               Object[] inputs) throws SqlException {
        boolean sendQryrowset = checkSendQryrowset(fetchSize, resultSetType);
        fetchSize = checkFetchsize(fetchSize, resultSetType);
        // think about if there is a way we can call build ddm just passing ddm parameters and not passing the material ps object
        // maybe not, if sometimes we need to set the caches hanging off the ps object during the ddm build
        // maybe we can extricate conditionals in the build ddm logic outside

        buildOPNQRY(section,
                sendQryrowset,
                fetchSize);


        // may be able to merge this with firstContinueQuery_ and push above conditional to common
        ((NetStatement) materialPreparedStatement).qryrowsetSentOnOpnqry_ = sendQryrowset;

        if (numInputColumns > 0) {
            if ((extdtaPositions_ != null) && (!extdtaPositions_.isEmpty())) {
                extdtaPositions_.clear();  // reset extdta column position markers
            }
            // is this the best place for this
            // EXCSQSTT needs this too

            // think about having this method return a boolean to
            // indicate the extdta should be built
            boolean overrideExists = buildSQLDTAcommandData(numInputColumns,
                    parameterMetaData,
                    inputs);

            // can we eleminate the chain argument needed for lobs
            // do we chain after Extdta's on open, verify this
            buildEXTDTA(parameterMetaData,
                    inputs,
                    false);  //chained, do we chain after Extdta's on open
        }
    }

    // Write the message to open a bound or prepared query without input parameters.
    // Check this-> For open query without input parameters
    public void writeOpenQuery(NetStatement materialStatement,
                               Section section,
                               int fetchSize,
                               int resultSetType) throws SqlException {
        boolean sendQryrowset = checkSendQryrowset(fetchSize, resultSetType);
        fetchSize = checkFetchsize(fetchSize, resultSetType);

        // think about if there is a way we can call build ddm just passing ddm parameters and not passing the material ps object
        // maybe not, if sometimes we need to set the caches hanging off the ps object during the ddm build
        // maybe we can extricate conditionals in the build ddm logic outside
        buildOPNQRY(section,
                sendQryrowset,
                fetchSize);


        // may be able to merge this with firstContinueQuery_ and push above conditional to common
        ((NetStatement) materialStatement).qryrowsetSentOnOpnqry_ = sendQryrowset; // net-specific event


    }

    // Write the message to peform a describe input.
    //

    public void writeDescribeInput(NetPreparedStatement materialPreparedStatement,
                                   Section section) throws SqlException {
        int typsqlda = CodePoint.TYPSQLDA_X_INPUT;

        buildDSCSQLSTT(section,
                true, //sendTypsqlda
                typsqlda);
    }

    // Write the message to peform a describe output.
    //
    // preconditions:
    public void writeDescribeOutput(NetPreparedStatement materialPreparedStatement,
                                    Section section) throws SqlException {
        // pick standard, light, extended sqlda. possibly push this up even more
        // right now use SQLAM level as determining factor and go for the most data.
        // if standard is the only suported option, don't send the typsqlda
        // and let server default to standard.  This prevents accidentally sending
        // a typsqlda to a downlevel server.  typsqlda is only supported at sqlam 6.
//KATHEY CHECK
        buildDSCSQLSTT(section,
                true, //sendTypsqlda
                CodePoint.TYPSQLDA_X_OUTPUT);  //typsqlda
    }

    // Write the message to execute a stored procedure.
    //
    // preconditions:
    public void writeExecuteCall(NetStatement materialStatement,
                                 boolean outputExpected,
                                 String procedureName,
                                 Section section,
                                 int fetchSize,
                                 boolean suppressResultSets, // for batch updates == true
                                 int resultSetType,
                                 ColumnMetaData parameterMetaData,
                                 Object[] inputs) throws SqlException // chain is for blobs
    {
        // always send QRYROWSET on EXCSQLSTT
        boolean sendQryrowset = true;
        fetchSize = (fetchSize == 0) ?
            Configuration.defaultFetchSize : fetchSize;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

        boolean sendPrcnam = (procedureName != null) ? true : false;
        int numParameters = (parameterMetaData != null) ? parameterMetaData.columns_ : 0;
        outputExpected = numParameters > 0;

        // is it right here to send maxblkext (-1)
        buildEXCSQLSTT(section,
                true, // sendOutexp
                outputExpected, // outexp
                sendPrcnam, // sendPrcnam
                procedureName, // prcnam
                true, // sendQryblksz
                !suppressResultSets, // sendMaxrslcnt,
                CodePoint.MAXRSLCNT_NOLIMIT, // maxrslcnt,
                true, // sendMaxblkext
                -1, // maxblkext (-1 for AR capable of receiving entire result set)
                true, // sendRslsetflg
                calculateResultSetFlags(), // resultSetFlag
                sendQryrowset, // sendQryrowset
                fetchSize);      // qryrowset

        if (numParameters > 0) {
            if ((extdtaPositions_ != null) && (!extdtaPositions_.isEmpty())) {
                extdtaPositions_.clear();  // reset extdta column position markers
            }
            // is this the best place for this (OPNQRY needs this too)

            // think about having this method return a boolean to
            // indicate the extdta should be built
            boolean overrideExists = buildSQLDTAcommandData(numParameters,
                    parameterMetaData,
                    inputs);

            buildEXTDTA(parameterMetaData, inputs, false); // no chained autocommit for CALLs
        }

        ((NetStatement) materialStatement).qryrowsetSentOnOpnqry_ = sendQryrowset;
    }

    // Write the message to execute an SQL Set Statement.
/*
  public void writeSetGenericSQLSetInfo (
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
         SetGenericSQLSetPiggybackCommand setGenericSQLSetPiggybackCommand,
         JDBCSection section) throws SqlException
  {
    buildEXCSQLSET (section);

    List sqlsttList = setGenericSQLSetPiggybackCommand.getList();
    for (int i = 0; i < sqlsttList.size(); i++) {
      String sql = (String)sqlsttList.get(i);
      // Build SQLSTT only for the SET statement that coming from the server after RLSCONV
      buildSQLSTTcommandData (sql);
    }
  }

*/
    //----------------------helper methods----------------------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // Build the Open Query Command to open a query to a relational database.
    // At SQLAM >= 7 we can request the return of a DA, are there
    // scenarios where this should currently be done (it is not supported now)
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    //
    private void buildOPNQRY(Section section,
                     boolean sendQueryRowSet,
                     int fetchSize) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.OPNQRY);

        buildPKGNAMCSN(section);
        buildQRYBLKSZ();  // specify a hard coded query block size

        if (sendQueryRowSet) {
            buildMAXBLKEXT(-1);
            buildQRYROWSET(fetchSize);
        }

        // Tell the server to close forward-only result sets
        // implicitly when they are exhausted. The server will ignore
        // this parameter if the result set is scrollable.
//IC see: https://issues.apache.org/jira/browse/DERBY-821
        if (netAgent_.netConnection_.serverSupportsQryclsimp()) {
            buildQRYCLSIMP();
        }

        updateLengthBytes();  // opnqry is complete
    }

    // Build the Execute Immediate SQL Statement Command to
    // execute a non-cursor SQL statement sent as command data.
    //
    // precondtions:
    private void buildEXCSQLIMM(Section section,
                        boolean sendQryinsid,
                        long qryinsid) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.EXCSQLIMM);

        buildPKGNAMCSN(section);
        buildRDBCMTOK();
        if (sendQryinsid) {
            buildQRYINSID(qryinsid);
        }

        updateLengthBytes();
    }

    // Build the Prepare SQL Statement Command to dynamically binds an
    // SQL statement to a section in an existing relational database (RDB) package.
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    private void buildPRPSQLSTT(Section section,
                        String sql,
                        boolean sendRtnsqlda,
                        boolean sendTypsqlda,
                        int typsqlda) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.PRPSQLSTT);

        buildPKGNAMCSN(section);
        if (sendRtnsqlda) {
            buildRTNSQLDA();
        }
        if (sendTypsqlda) {
            buildTYPSQLDA(typsqlda);
        }

        updateLengthBytes();
    }

    // Build the command to execute an SQL SET Statement.
    // Called by NETSetClientPiggybackCommand.write()
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    private void buildEXCSQLSET(Section section)
            throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.EXCSQLSET);
        buildPKGNAMCSN(section);  // is this PKGNAMCSN or PKGNAMCT
        updateLengthBytes();
    }

    // Build the Execute SQL Statement (EXCSQLSTT) Command
    // to execute a non-cursor SQL statement previously bound into a named package
    // of a relational database (RDB).  The SQL statement can optionally include
    // references to input variables, output variables, or both.
    //
    // At SQLAM >= 7 we can get a DA back on this, are there times that we want to request it
    // If so, we probably want to pass a parameter indicating the sqldaLevel requested.
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    // Here is the preferred codepoint ordering:
    // PKGNAMCSN
    // RDBCMTOK
    // OUTEXP
    // QRYBLKSZ
    // MAXBLKEXT
    // MAXRSLCNT
    // RSLSETFLG
    // QRYROWSET
    // RTNSQLDA
    // TYPSQLDA
    // NBRROW
    // ATMIND
    // PRCNAM
    // OUTOVROPT
    // RDBNAM
    private void buildEXCSQLSTT(Section section,
                        boolean sendOutexp,
                        boolean outexp,
                        boolean sendPrcnam,
                        String prcnam,
                        boolean sendQryblksz,
                        boolean sendMaxrslcnt,
                        int maxrslcnt,
                        boolean sendMaxblkext,
                        int maxblkext,
                        boolean sendRslsetflg,
                        int resultSetFlag,
                        boolean sendQryrowset,
                        int qryrowset) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.EXCSQLSTT);

        buildPKGNAMCSN(section);
        buildRDBCMTOK();
        if (sendOutexp) {
            buildOUTEXP(outexp);
        }
        if (sendQryblksz) {
            buildQRYBLKSZ();
        }
        if (sendQryrowset && sendMaxblkext) {
            buildMAXBLKEXT(maxblkext);
        }
        if (sendMaxrslcnt) {
            buildMAXRSLCNT(maxrslcnt);
        }
        if (sendRslsetflg) {
            buildRSLSETFLG(resultSetFlag);
        }
        if (sendQryrowset) {
            buildQRYROWSET(qryrowset);
        }
        if (sendPrcnam) {
            buildPRCNAM(prcnam);
        }

        updateLengthBytes();  // command is complete, update the length bytes
    }

    // Build the Describe SQL Statement command.
    //
    // preconditions:
    //   the sqlam and/or prdid must support command and parameters passed to this method,
    //   method will not validate against the connection's level of support
    private void buildDSCSQLSTT(Section section,
                        boolean sendTypsqlda,
                        int typsqlda) throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.DSCSQLSTT);

        buildPKGNAMCSN(section);
        if (sendTypsqlda) {
            buildTYPSQLDA(typsqlda);
        }

        updateLengthBytes();
    }

    // Build the SQL Program Variable Data Command Data Object.
    // This object contains the input data to an SQL statement
    // that an RDB is executing.
    //
    // preconditions:
    private boolean buildSQLDTAcommandData(int numInputColumns,
                                   ColumnMetaData parameterMetaData,
                                   Object[] inputRow) throws SqlException {
        createEncryptedCommandData();

        int loc = buffer.position();

        markLengthBytes(CodePoint.SQLDTA);

        int[][] protocolTypesAndLengths = allocateLidAndLengthsArray(parameterMetaData);

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        Hashtable protocolTypeToOverrideLidMapping = null;
        ArrayList mddOverrideArray = null;
        protocolTypeToOverrideLidMapping =
                computeProtocolTypesAndLengths(inputRow, parameterMetaData, protocolTypesAndLengths,
                        protocolTypeToOverrideLidMapping);

        boolean overrideExists = false;

        buildFDODSC(numInputColumns,
                protocolTypesAndLengths,
                overrideExists,
                protocolTypeToOverrideLidMapping,
                mddOverrideArray);

        buildFDODTA(numInputColumns,
                protocolTypesAndLengths,
                inputRow);

        updateLengthBytes(); // for sqldta
        if (netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRPWDDTA) {
            encryptDataStream(loc);
        }

        return overrideExists;
    }

    // Build the FDOCA Data Descriptor Scalar whose value is a FDOCA
    // Descriptor or a segment of an FDOCA Descriptor.
    //
    // preconditions:
    private void buildFDODSC(int numColumns,
                             int[][] protocolTypesAndLengths,
                             boolean overrideExists,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                             Hashtable overrideMap,
                             ArrayList overrideArray) throws SqlException {
        markLengthBytes(CodePoint.FDODSC);
        buildSQLDTA(numColumns, protocolTypesAndLengths, overrideExists, overrideMap, overrideArray);
        updateLengthBytes();
    }

    // Build the FDOCA SQLDTA Late Row Descriptor.
    //
    // preconditions:
    private void buildSQLDTA(int numColumns,
                               int[][] lidAndLengthOverrides,
                               boolean overrideExists,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                               Hashtable overrideMap,
                               ArrayList overrideArray) throws SqlException {
        // mdd overrides need to be built first if any before the descriptors are built.
        if (overrideExists) {
            buildMddOverrides(overrideArray);
            writeBytes(FdocaConstants.MDD_SQLDTAGRP_TOSEND);
        }

        buildSQLDTAGRP(numColumns, lidAndLengthOverrides, overrideExists, overrideMap);

        if (overrideExists) {
            writeBytes(FdocaConstants.MDD_SQLDTA_TOSEND);
        }
        writeBytes(FdocaConstants.SQLDTA_RLO_TOSEND);
    }

    // Build the FDOCA SQLDTAGRP Late Group Descriptor.
    // preconditions:
    private void buildSQLDTAGRP(int numVars,
                                  int[][] lidAndLengthOverrides,
                                  boolean mddRequired,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                  Hashtable overrideMap) throws SqlException {
        int n = 0;
        int offset = 0;

        n = calculateColumnsInSQLDTAGRPtriplet(numVars);
        buildTripletHeader(((3 * n) + 3),
                FdocaConstants.NGDA_TRIPLET_TYPE,
                FdocaConstants.SQLDTAGRP_LID);

        do {
            writeLidAndLengths(lidAndLengthOverrides, n, offset, mddRequired, overrideMap);
            numVars -= n;
            if (numVars == 0) {
                break;
            }

            offset += n;
            n = calculateColumnsInSQLDTAGRPtriplet(numVars);
            buildTripletHeader(((3 * n) + 3),
                    FdocaConstants.CPT_TRIPLET_TYPE,
                    0x00);
        } while (true);
    }

/////////// perf end


    protected void buildOUTOVR(ClientResultSet resultSet,
                               ColumnMetaData resultSetMetaData) throws SqlException {
        createCommandData();
        markLengthBytes(CodePoint.OUTOVR);
        int[][] outputOverrides =
                calculateOUTOVRLidAndLengthOverrides(resultSet, resultSetMetaData);
        buildSQLDTARD(resultSetMetaData.columns_, outputOverrides);
        updateLengthBytes();
    }

    private int[][] calculateOUTOVRLidAndLengthOverrides(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            ClientResultSet resultSet,
            ColumnMetaData resultSetMetaData) {

        int numVars = resultSetMetaData.columns_;
        int[][] lidAndLengths = new int[numVars][2]; //everything initialized to "default triplet"

        for (int i=0; i<numVars; ++i) {
            switch (resultSetMetaData.types_[i]) {
                case Types.BLOB:
                    lidAndLengths[i][0] = (resultSetMetaData.nullable_[i])
                            ? DRDAConstants.DRDA_TYPE_NLOBLOC
                            : DRDAConstants.DRDA_TYPE_LOBLOC;
                    lidAndLengths[i][1] = 4;
                    break;
 
                case Types.CLOB:
                    lidAndLengths[i][0] = (resultSetMetaData.nullable_[i])
//IC see: https://issues.apache.org/jira/browse/DERBY-2702
                            ? DRDAConstants.DRDA_TYPE_NCLOBLOC
                            : DRDAConstants.DRDA_TYPE_CLOBLOC;
                    lidAndLengths[i][1] = 4;
                    break;
            }
        }

        return lidAndLengths;
    }

    private void buildSQLDTARD(int numColumns, int[][] lidAndLengthOverrides)
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SqlException {
        buildSQLCADTA(numColumns, lidAndLengthOverrides);
        writeBytes(FdocaConstants.SQLDTARD_RLO_TOSEND);
    }

    private void buildSQLCADTA(int numColumns, int[][] lidAndLengthOverrides)
            throws SqlException {
        buildSQLDTAGRP(numColumns, lidAndLengthOverrides, false, null);  // false means no mdd override
        writeBytes(FdocaConstants.SQLCADTA_RLO_TOSEND);
    }

    private void buildFDODTA(int numVars,
                             int[][] protocolTypesAndLengths,
                             Object[] inputs) throws SqlException {
        try
        {
            
            Object o = null;

            markLengthBytes(CodePoint.FDODTA);
            write1Byte(FdocaConstants.NULL_LID); // write the 1-byte row indicator

            // write data for each input column
            for (int i = 0; i < numVars; i++) {
                if (inputs[i] == null) {
                    if ((protocolTypesAndLengths[i][0] % 2) == 1) {
                        write1Byte(FdocaConstants.NULL_DATA);
                    } else {
                        //bug check
                    }
                } else {
                    if ((protocolTypesAndLengths[i][0] % 2) == 1) {
                        write1Byte(FdocaConstants.INDICATOR_NULLABLE);
                    }

                    switch (protocolTypesAndLengths[i][0] | 0x01) {  // mask out null indicator
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NVARMIX:
                    case DRDAConstants.DRDA_TYPE_NLONGMIX:
                        // What to do for server that don't understand 1208 (UTF-8)
                        // check for a promototed type, and use that instead if it exists
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {
                            writeSingleorMixedCcsidLDString((String) inputs[i], netAgent_.typdef_.getCcsidMbcEncoding());
                        } else { // use the promototed object instead
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
                                         (ClientClob) o,
                                         protocolTypesAndLengths,
                                         i);
                        }
                        break;

//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NVARCHAR:
                    case DRDAConstants.DRDA_TYPE_NLONG:
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {

                        } else { // use the promototed object instead
                            
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
                                         (ClientClob) o,
                                         protocolTypesAndLengths,
                                         i);
                            
                        }
                        break;

                    case DRDAConstants.DRDA_TYPE_NBOOLEAN:
                        writeBoolean(((Boolean) inputs[i]).booleanValue());
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NINTEGER:
                        writeIntFdocaData(((Integer) inputs[i]).intValue());
                        break;
                    case DRDAConstants.DRDA_TYPE_NSMALL:
                        writeShortFdocaData(((Short) inputs[i]).shortValue());
                        break;
                    case DRDAConstants.DRDA_TYPE_NFLOAT4:
                        writeFloat(((Float) inputs[i]).floatValue());
                        break;
                    case DRDAConstants.DRDA_TYPE_NFLOAT8:
                        writeDouble(((Double) inputs[i]).doubleValue());
                        break;
                    case DRDAConstants.DRDA_TYPE_NDECIMAL:
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        writeBigDecimal((BigDecimal) inputs[i],
                                (protocolTypesAndLengths[i][1] >> 8) & 0xff, // described precision not actual
                                protocolTypesAndLengths[i][1] & 0xff); // described scale, not actual
                        break;
                    case DRDAConstants.DRDA_TYPE_NDATE:
                        // The value may be a Date if it comes from one of the
                        // methods that don't specify the calendar, or a
                        // DateTimeValue if it comes from a method that does
                        // specify the calendar. Convert to DateTimeValue if
                        // needed.
                        DateTimeValue dateVal = (inputs[i] instanceof Date) ?
                                    new DateTimeValue((Date) inputs[i]) :
                                    (DateTimeValue) inputs[i];
                        writeDate(dateVal);
                        break;
                    case DRDAConstants.DRDA_TYPE_NTIME:
                        // The value may be a Time if it comes from one of the
                        // methods that don't specify the calendar, or a
                        // DateTimeValue if it comes from a method that does
                        // specify the calendar. Convert to DateTimeValue if
                        // needed.
                        DateTimeValue timeVal = (inputs[i] instanceof Time) ?
                                    new DateTimeValue((Time) inputs[i]) :
                                    (DateTimeValue) inputs[i];
                        writeTime(timeVal);
                        break;
                    case DRDAConstants.DRDA_TYPE_NTIMESTAMP:
                        // The value may be a Timestamp if it comes from one of
                        // the methods that don't specify the calendar, or a
                        // DateTimeValue if it comes from a method that does
                        // specify the calendar. Convert to DateTimeValue if
                        // needed.
                        DateTimeValue tsVal = (inputs[i] instanceof Timestamp) ?
                                    new DateTimeValue((Timestamp) inputs[i]) :
                                    (DateTimeValue) inputs[i];
                        writeTimestamp(tsVal);
                        break;
                    case DRDAConstants.DRDA_TYPE_NINTEGER8:
                        writeLongFdocaData(((Long) inputs[i]).longValue());
                        break;
                    case DRDAConstants.DRDA_TYPE_NVARBYTE:
                    case DRDAConstants.DRDA_TYPE_NLONGVARBYTE:
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {
                            writeLDBytes((byte[]) inputs[i]);
                        } else { // use the promototed object instead
                            
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
                                         (ClientClob) o,
                                         protocolTypesAndLengths,
                                         i);
                        }
                        break;
                    case DRDAConstants.DRDA_TYPE_NUDT:
                        writeUDT( inputs[i] );
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NLOBCSBCS:
                    case DRDAConstants.DRDA_TYPE_NLOBCDBCS:
                        // check for a promoted Clob
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {
                            try {

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                Clob c = (Clob) inputs[i];
                                
                                if(c instanceof ClientClob &&
                                    ((ClientClob)c).willBeLayerBStreamed()) {

                                    setFDODTALobLengthUnknown(i);
                                } else {
                                    long dataLength = c.length();
                                    setFDODTALobLength(protocolTypesAndLengths, i, dataLength);

                                }
                                
                            } catch (SQLException e) {
                                throw new SqlException(netAgent_.logWriter_, 
                                    new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH),
                                    e);
                            }
                        } else {
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
                                         (ClientClob) o,
                                         protocolTypesAndLengths,
                                         i);
                        }
                        
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NLOBBYTES:
                        // check for a promoted Clob
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {
                            try {

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                Blob b = (Blob) inputs[i];
                                
                                if(b instanceof ClientBlob &&
                                   ((ClientBlob)b).willBeLayerBStreamed()) {
                                    
                                    setFDODTALobLengthUnknown( i );
                                } else {
                                    long dataLength = b.length();
                                    setFDODTALobLength(protocolTypesAndLengths, i, dataLength);
                                    
                                }
                                
                            } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-848
                                throw new SqlException(netAgent_.logWriter_, 
                                    new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH),
                                    e);
                            }
                        } else { // use promoted Blob
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
                                         (ClientBlob) o,
                                         protocolTypesAndLengths,
                                         i);
                        }
                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    case DRDAConstants.DRDA_TYPE_NLOBCMIXED:
                        // check for a promoted Clob
                        o = retrievePromotedParameterIfExists(i);
                        if (o == null) {
                            
                            final ClientClob c = (ClientClob) inputs[i];
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

                            if (c.isString()) {
                                setFDODTALobLength(protocolTypesAndLengths, 
                                                   i, 
                                                   c.getUTF8Length() );
                                
                            } else if ( ! c.willBeLayerBStreamed() ){
                                // must be a Unicode stream
                                setFDODTALobLength(protocolTypesAndLengths, 
                                                   i, 
                                                   c.length() );
                                
                            } else {
                                setFDODTALobLengthUnknown( i );
                                
                            }
                            
                        } else { // use promoted Clob
                            setFDODTALob(netAgent_.netConnection_.getSecurityMechanism(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                         (ClientClob) o,
                                         protocolTypesAndLengths,
                                         i);
                        }

                        break;
//IC see: https://issues.apache.org/jira/browse/DERBY-2506
                    case DRDAConstants.DRDA_TYPE_NLOBLOC:
                        //The FD:OCA data or the FDODTA contains the locator
                        //value corresponding to the LOB. write the integer
                        //value representing the locator here.
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        writeIntFdocaData(((ClientBlob)inputs[i]).
                                getLocator());
                        break;
                    case DRDAConstants.DRDA_TYPE_NCLOBLOC:
                        //The FD:OCA data or the FDODTA contains the locator
                        //value corresponding to the LOB. write the integer
                        //value representing the locator here.
                        writeIntFdocaData(((ClientClob)inputs[i]).
                                getLocator());
                        break;
                    default:
                        throw new SqlException(netAgent_.logWriter_, 
                            new ClientMessageId(SQLState.NET_UNRECOGNIZED_JDBC_TYPE),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                               protocolTypesAndLengths[i][0], numVars, i);
                    }
                }
            }
            updateLengthBytes(); // for fdodta
        }
        catch ( SQLException se )
        {
            throw new SqlException(se);
        }
    }

    // preconditions:
    private void buildEXTDTA(ColumnMetaData parameterMetaData,
                             Object[] inputRow,
                             boolean chained) throws SqlException {
        try
        {
            // build the EXTDTA data, if necessary
            if (extdtaPositions_ != null) {
                boolean chainFlag, chainedWithSameCorrelator;

                for (int i = 0; i < extdtaPositions_.size(); i++) {
                    int index = extdtaPositions_.get(i);

                    // is this the last EXTDTA to be built?
                    if (i != extdtaPositions_.size() - 1) { // no
                        chainFlag = true;
                        chainedWithSameCorrelator = true;
                    } else { // yes
                        chainFlag = chained;
                        chainedWithSameCorrelator = false;
                    }

                    // do we have to write a null byte?
                    boolean writeNullByte = false;
                    if (parameterMetaData.nullable_[index]) {
                        writeNullByte = true;
                    }
                    // Use the type of the input parameter rather than the input
                    // column if possible.
                    int parameterType = parameterMetaData.clientParamtertype_[index];
                    if (parameterType == 0) {
                        parameterType = parameterMetaData.types_[index];
                    }

                    // the follow types are possible due to promotion to BLOB
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                    if (parameterType == ClientTypes.BLOB
                            || parameterType == ClientTypes.BINARY
                            || parameterType == ClientTypes.VARBINARY
                            || parameterType == ClientTypes.LONGVARBINARY) {

                        ClientBlob o =
                          (ClientBlob)retrievePromotedParameterIfExists(index);

                        Blob b = (o == null) ? (Blob) inputRow[index] : o;
                        boolean isExternalBlob = !(b instanceof ClientBlob);
                        if (isExternalBlob) {
                            try {
                                writeScalarStream(chainFlag,
                                        chainedWithSameCorrelator,
                                        CodePoint.EXTDTA,
                                        (int) b.length(),
                                        b.getBinaryStream(),
                                        writeNullByte,
                                        index + 1);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-848
//IC see: https://issues.apache.org/jira/browse/DERBY-848
                                throw new SqlException(netAgent_.logWriter_, 
                                    new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH),
                                    e);
                            }
                        } else if ( ( (ClientBlob) b).isBinaryStream()) {
                            
                            if( ( (ClientBlob) b).willBeLayerBStreamed() ){
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    ((ClientBlob)b).getBinaryStream(),
                                    writeNullByte,
                                    index + 1);
                            }else{
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    (int)((ClientBlob)b).length(),
                                    ((ClientBlob)b).getBinaryStream(),
                                    writeNullByte,
                                    index + 1);
                            }
                        } else { // must be a binary string
                            // note: a possible optimization is to use writeScalarLobBytes
                            //       when the input is small
                            //   use this: if (b.length () < DssConstants.MAX_DSS_LEN - 6 - 4)
                            //               writeScalarLobBytes (...)
                            // Yes, this would avoid having to new up a java.io.ByteArrayInputStream
                            writeScalarStream(chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                                    (int) ((ClientBlob) b).length(),
                                    ((ClientBlob) b).getBinaryStream(),
                                    writeNullByte,
                                    index + 1);
                        }
                    }
                    // the follow types are possible due to promotion to CLOB
                    else if (
                            parameterType == ClientTypes.CLOB
                            || parameterType == ClientTypes.CHAR
                            || parameterType == ClientTypes.VARCHAR
                            || parameterType == ClientTypes.LONGVARCHAR) {

                        ClientClob o =
                          (ClientClob)retrievePromotedParameterIfExists(index);

                        Clob c = (o == null) ? (Clob) inputRow[index] : o;
                        boolean isExternalClob = !(c instanceof ClientClob);

                        if (isExternalClob) {
                            try {
                                writeScalarStream(chainFlag,
                                        chainedWithSameCorrelator,
                                        CodePoint.EXTDTA,
                                        (int) c.length(),
                                        c.getCharacterStream(),
                                        writeNullByte,
                                        index + 1);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            } catch (SQLException e) {
                                throw new SqlException(netAgent_.logWriter_, 
                                    new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH),
                                    e);
                            }
                        } else if ( ( (ClientClob) c).isCharacterStream()) {
                            
                            if( ( (ClientClob) c).willBeLayerBStreamed() ) {
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    ((ClientClob)c).getCharacterStream(),
                                    writeNullByte,
                                    index + 1);
                            }else{
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    (int)((ClientClob)c).length(),
                                    ((ClientClob)c).getCharacterStream(),
                                    writeNullByte,
                                    index + 1);
                            }
                            
                        } else if (((ClientClob) c).isAsciiStream()) {
                            
                            if( ( (ClientClob) c).willBeLayerBStreamed() ){
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    ((ClientClob)c).getAsciiStream(),
                                    writeNullByte,
                                    index + 1);
                            }else { 
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    (int)((ClientClob)c).length(),
                                    ((ClientClob)c).getAsciiStream(),
                                    writeNullByte,
                                    index + 1);
                            }
                                
                        } else if (((ClientClob) c).isUnicodeStream()) {
                            
                            if( ( (ClientClob) c).willBeLayerBStreamed() ){
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    ((ClientClob)c).getUnicodeStream(),
                                    writeNullByte,
                                    index + 1);
                            }else{
                                writeScalarStream(
                                    chainFlag,
                                    chainedWithSameCorrelator,
                                    CodePoint.EXTDTA,
                                    (int)((ClientClob)c).length(),
                                    ((ClientClob)c).getUnicodeStream(),
                                    writeNullByte,
                                    index + 1);
                            }
                        } else { // must be a String
                            // note: a possible optimization is to use writeScalarLobBytes
                            //       when the input is small.
                            //   use this: if (c.length () < DssConstants.MAX_DSS_LEN - 6 - 4)
                            //               writeScalarLobBytes (...)
                            writeScalarStream(
                                chainFlag,
                                chainedWithSameCorrelator,
                                CodePoint.EXTDTA,
                                (int)((ClientClob)c).getUTF8Length(),
                                new ByteArrayInputStream(
                                    ((ClientClob) c).getUtf8String()),
                                writeNullByte,
                                index + 1);
                        }
                    }
                }
            }
        }
        catch ( SQLException se )
        {
            throw new SqlException(se);
        }
    }


    //-------------------------helper methods-------------------------------------
    // returns the a promototedParameter object for index or null if it does not exist
    private Object retrievePromotedParameterIfExists(int index) {

        // consider using a nonsynchronized container or array
        if (promototedParameters_.isEmpty()) {
            return null;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return promototedParameters_.get(index);
    }

    private int calculateColumnsInSQLDTAGRPtriplet(int numVars) {
        if (numVars > FdocaConstants.MAX_VARS_IN_NGDA) //rename to MAX_VARS_IN_SQLDTAGRP_TRIPLET
        {
            return FdocaConstants.MAX_VARS_IN_NGDA;
        }
        return numVars;
    }


    // Consider refacctor so that this does not even have to look
    // at the actual object data, and only uses tags from the meta data
    // only have to call this once, rather than calling this for every input row
    // Comment: I don't think that it is possible to send decimal values without looking at the data for 
    // precision and scale (Kathey Marsden 10/11)
    // backburner: after refactoring this, later on, think about replacing case statements with table lookups
    private Hashtable computeProtocolTypesAndLengths(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            Object[] inputRow,
            ColumnMetaData parameterMetaData,
            int[][] lidAndLengths,
            Hashtable overrideMap) throws SqlException {

        try
        {
            int numVars = parameterMetaData.columns_;
            String s = null;
            if (!promototedParameters_.isEmpty()) {
                promototedParameters_.clear();
            }

            for (int i = 0; i < numVars; i++) {

                int jdbcType;
                // Send the input type unless it is not available.
                // (e.g an output parameter)
                jdbcType = parameterMetaData.clientParamtertype_[i];
                if (jdbcType == 0) {
                    jdbcType = parameterMetaData.types_[i];
                }

                // jdbc semantics - This should happen outside of the build methods
                // if describe input is not supported, we require the user to at least
                // call setNull() and provide the type information.  Otherwise, we won't
                // be able to guess the right PROTOCOL type to send to the server, and an
                // exception is thrown.

                if (jdbcType == 0) {
                    throw new SqlException(netAgent_.logWriter_, 
                        new ClientMessageId(SQLState.NET_INVALID_JDBC_TYPE_FOR_PARAM),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        i);
                }

                switch (jdbcType) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                case Types.CHAR:
                case Types.VARCHAR:
                    // lid: PROTOCOL_TYPE_NVARMIX, length override: 32767 (max)
                    // dataFormat: String
                    // this won't work if 1208 is not supported
                    s = (String) inputRow[i];
                    // assumes UTF-8 characters at most 3 bytes long
                    // Flow the String as a VARCHAR
                    if (s == null || s.length() <= 32767 / 3) {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NVARMIX;
                        lidAndLengths[i][1] = 32767;
                    } else {
                        // Flow the data as CLOB data if the data too large to for LONGVARCHAR
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                        byte[] ba = s.getBytes(Typdef.UTF8ENCODING);
                        ByteArrayInputStream bais = new ByteArrayInputStream(ba);
                        ClientClob c = new ClientClob(
                            netAgent_, bais, Typdef.UTF8ENCODING, ba.length);
                        // inputRow[i] = c;
                        // Place the new Lob in the promototedParameter_ collection for
                        // NetStatementRequest use
                        promototedParameters_.put(i, c);

                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCMIXED;

                        if( c.willBeLayerBStreamed() ){

                            //Correspond to totalLength 0 as default length for unknown
                            lidAndLengths[i][1] = 0x8002;

                        }else {
                            lidAndLengths[i][1] = buildPlaceholderLength(c.length());

                        }
                    }
                    break;
                case Types.INTEGER:
                    // lid: PROTOCOL_TYPE_NINTEGER, length override: 4
                    // dataFormat: Integer
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NINTEGER;
                    lidAndLengths[i][1] = 4;
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    if ( netAgent_.netConnection_.databaseMetaData_.
                            serverSupportsBooleanParameterTransport() )
                    {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NBOOLEAN;
                        lidAndLengths[i][1] = 1;
                    }
                    else
                    {
                        // If the server doesn't support BOOLEAN parameters,
                        // send the parameter as a SMALLINT instead.
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NSMALL;
                        lidAndLengths[i][1] = 2;
                        if (inputRow[i] instanceof Boolean) {
                            Boolean bool = (Boolean) inputRow[i];
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                            inputRow[i] = Short.valueOf(
                                    bool.booleanValue() ? (short) 1 : 0);
                        }
                    }
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                case Types.SMALLINT:
                case Types.TINYINT:
                    // lid: PROTOCOL_TYPE_NSMALL,  length override: 2
                    // dataFormat: Short
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NSMALL;
                    lidAndLengths[i][1] = 2;
                    break;
                case Types.REAL:
                    // lid: PROTOCOL_TYPE_NFLOAT4, length override: 4
                    // dataFormat: Float
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NFLOAT4;
                    lidAndLengths[i][1] = 4;
                    break;
                case Types.DOUBLE:
                case Types.FLOAT:
                    // lid: PROTOCOL_TYPE_NFLOAT8, length override: 8
                    // dataFormat: Double
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NFLOAT8;
                    lidAndLengths[i][1] = 8;
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    // lid: PROTOCOL_TYPE_NDECIMAL
                    // dataFormat: java.math.BigDecimal
                    // if null - guess with precision 1, scale 0
                    // if not null - get scale from data and calculate maximum precision.
                    // DERBY-2073. Get scale and precision from data so we don't lose fractional digits.
                    BigDecimal bigDecimal = (BigDecimal) inputRow[i];
                    int scale;
                    int precision;
                    
//IC see: https://issues.apache.org/jira/browse/DERBY-3126
                    if (bigDecimal == null)
                    {
                        scale = 0;
                        precision = 1;
                    }
                    else
                    {
                        // adjust scale if it is negative. Packed Decimal cannot handle 
                        // negative scale. We don't want to change the original 
                        // object so make a new one.
                        if (bigDecimal.scale() < 0) 
                        {
                            bigDecimal =  bigDecimal.setScale(0);
                            inputRow[i] = bigDecimal;
                        }                        
                        scale = bigDecimal.scale();
                        precision = Utils.computeBigDecimalPrecision(bigDecimal);
                    }                    
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NDECIMAL;
                    lidAndLengths[i][1] = (precision << 8) + // use precision above
                        (scale << 0);
                    
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                case Types.DATE:
                    // for input, output, and inout parameters
                    // lid: PROTOCOL_TYPE_NDATE, length override: 8
                    // dataFormat: java.sql.Date
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NDATE;
                    lidAndLengths[i][1] = 10;
                    break;
                case Types.TIME:
                    // for input, output, and inout parameters
                    // lid: PROTOCOL_TYPE_NTIME, length override: 8
                    // dataFormat: java.sql.Time
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NTIME;
                    lidAndLengths[i][1] = 8;
                    break;
                case Types.TIMESTAMP:
                    // for input, output, and inout parameters
                    // lid: PROTOCOL_TYPE_NTIMESTAMP, length overrid: 26 or 29
                    // dataFormat: java.sql.Timestamp
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NTIMESTAMP;
                    lidAndLengths[i][1] = DateTime.getTimestampLength( netAgent_.netConnection_.serverSupportsTimestampNanoseconds() );
                    break;
                case Types.BIGINT:
                    // if SQLAM < 6 this should be mapped to decimal (19,0) in common layer
                    // if SQLAM >=6, lid: PROTOCOL_TYPE_NINTEGER8, length override: 8
                    // dataFormat: Long
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NINTEGER8;
                    lidAndLengths[i][1] = 8;
                    break;
                case Types.LONGVARCHAR:
                    // Is this the right thing to do  // should this be 32700
                    s = (String) inputRow[i];
                    if (s == null || s.length() <= 32767 / 3) {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLONGMIX;
                        lidAndLengths[i][1] = 32767;
                    } else {
                        // Flow the data as CLOB data if the data too large to for LONGVARCHAR
//IC see: https://issues.apache.org/jira/browse/DERBY-6231
                        byte[] ba = s.getBytes(Typdef.UTF8ENCODING);
                        ByteArrayInputStream bais = new ByteArrayInputStream(ba);
                        ClientClob c = new ClientClob(
                            netAgent_, bais, Typdef.UTF8ENCODING, ba.length);

                        // inputRow[i] = c;
                        // Place the new Lob in the promototedParameter_ collection for
                        // NetStatementRequest use
                        promototedParameters_.put(i, c);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

//IC see: https://issues.apache.org/jira/browse/DERBY-499
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCMIXED;
                        lidAndLengths[i][1] = buildPlaceholderLength(c.length());
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    byte[] ba = (byte[]) inputRow[i];
                    if (ba == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NVARBYTE;
                        lidAndLengths[i][1] = 32767;
                    } else if (ba.length <= 32767) {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NVARBYTE;
                        lidAndLengths[i][1] = 32767;
                    } else {
                        // Promote to a BLOB. Only reach this path in the absence of describe information.
                        ClientBlob b = new ClientBlob(ba, netAgent_, 0);

                        // inputRow[i] = b;
                        // Place the new Lob in the promototedParameter_ collection for
                        // NetStatementRequest use
                        promototedParameters_.put(i, b);

//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBBYTES;
                        lidAndLengths[i][1] = buildPlaceholderLength(ba.length);
                    }
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                case Types.LONGVARBINARY:
                    ba = (byte[]) inputRow[i];
                    if (ba == null) {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLONGVARBYTE;
                        lidAndLengths[i][1] = 32767;
                    } else if (ba.length <= 32767) {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLONGVARBYTE;
                        lidAndLengths[i][1] = 32767;
                    } else {
                        // Promote to a BLOB. Only reach this path in the absensce of describe information.
                        ClientBlob b = new ClientBlob(ba, netAgent_, 0);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

                        // inputRow[i] = b;
                        // Place the new Lob in the promototedParameter_ collection for
                        // NetStatementRequest use
                        promototedParameters_.put(i, b);
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

//IC see: https://issues.apache.org/jira/browse/DERBY-499
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBBYTES;
                        lidAndLengths[i][1] = buildPlaceholderLength(ba.length);
                    }
                    break;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                case Types.JAVA_OBJECT:
                    lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NUDT;
                    lidAndLengths[i][1] = 32767;
                    break;
                case Types.BLOB:
                    Blob b = (Blob) inputRow[i];
                    if (b == null) {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBBYTES;
                        lidAndLengths[i][1] =
                                buildPlaceholderLength(parameterMetaData.sqlLength_[i]);
                    } else if (b instanceof ClientBlob &&
                               ((ClientBlob)b).isLocator()) {
                        //we are sending locators.
                        //Here the LID local identifier in the FDODSC
                        //FD:OCA descriptor should be initialized as
                        //to contain a BLOB locator.
//IC see: https://issues.apache.org/jira/browse/DERBY-2506
                        lidAndLengths[i][0] = 
                                    DRDAConstants.DRDA_TYPE_NLOBLOC;
                        lidAndLengths[i][1] = 4;
                    } else {
                        lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBBYTES;
                        try {
                            
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            if( b instanceof ClientBlob &&
                                ( (ClientBlob) b).willBeLayerBStreamed() ){
                                
                                //Correspond to totalLength 0 as default length for unknown
                                lidAndLengths[i][1] = 0x8002;
                                    
                            }else {
                                lidAndLengths[i][1] = buildPlaceholderLength(b.length());
                                
                            }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-848
                            throw new SqlException(netAgent_.logWriter_, 
                                new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH), e);
                        }
                    }
                    break;
                case Types.CLOB:
                    {
                        // use columnMeta.singleMixedByteOrDouble_ to decide protocolType
                        Clob c = (Clob) inputRow[i];
                        boolean isExternalClob = !(c instanceof ClientClob);
                        long lobLength = 0;
                        
                        boolean doesLayerBStreaming = false;
                        
                        if (c == null) {
                            lobLength = parameterMetaData.sqlLength_[i];
                        } else if (c instanceof ClientClob &&
                                   ((ClientClob)c).isLocator()) {
                            //The inputRow contains an Integer meaning that
                            //we are sending locators.
                            //Here the LID local identifier in the FDODSC
                            //FD:OCA descriptor should be initialized as
                            //to contain a CLOB locator.
//IC see: https://issues.apache.org/jira/browse/DERBY-2506
                            lidAndLengths[i][0] = 
                                    DRDAConstants.DRDA_TYPE_NCLOBLOC;
                            lidAndLengths[i][1] = 4;
                        } else if (isExternalClob) {
                            try {
                                lobLength = c.length();
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                            } catch (SQLException e) {
//IC see: https://issues.apache.org/jira/browse/DERBY-848
//IC see: https://issues.apache.org/jira/browse/DERBY-848
                                throw new SqlException(netAgent_.logWriter_, 
                                    new ClientMessageId(SQLState.NET_ERROR_GETTING_BLOB_LENGTH),
                                    e);
                            }
                        } else {
                            if( ( (ClientClob) c ).willBeLayerBStreamed() ){
                                doesLayerBStreaming = true;
                                
                            }else{
                                lobLength = ((ClientClob) c).length();
                                
                            }
                            
                        }
                        
                        if (c == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-499
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCMIXED;
                            lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
                        } else if (isExternalClob) {
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCDBCS;
                            lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        } else if (((ClientClob) c).isCharacterStream()) {
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCDBCS;
                            
                            if( doesLayerBStreaming ) {
                                
                                //Correspond to totalLength 0 as default length for unknown
                                lidAndLengths[i][1] = 0x8002;
                                
                            }else {
                                lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
                            }
                            
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        } else if (((ClientClob) c).isUnicodeStream()) {
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCMIXED;
                            
                            if( doesLayerBStreaming ) {
                                
                                //Correspond to totalLength 0 as default length for unknown
                                lidAndLengths[i][1] = 0x8002;
                                
                            }else {
                                lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
                            }
                            
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        } else if (((ClientClob) c).isAsciiStream()) {
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCSBCS;

                            if( doesLayerBStreaming ) {

                                //Correspond to totalLength 0 as default length for unknown
                                lidAndLengths[i][1] = 0x8002;
                                
                            }else {
                                lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
                            }
                            
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
                        } else if (((ClientClob) c).isString()) {
                            lidAndLengths[i][0] = DRDAConstants.DRDA_TYPE_NLOBCMIXED;
                            
                            if( doesLayerBStreaming ) {
                                
                                //Correspond to totalLength 0 as default length for unknown
                                lidAndLengths[i][1] = 0x8002;
                                
                            }else {
                                lidAndLengths[i][1] = buildPlaceholderLength(lobLength);
                            }
                            
                        }
                    }
                    break;
                default :
                    throw new SqlException(netAgent_.logWriter_, 
                        new ClientMessageId(SQLState.UNRECOGNIZED_JAVA_SQL_TYPE),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                        jdbcType);
                }

                if (!parameterMetaData.nullable_[i]) {
                    lidAndLengths[i][0]--;
                }
            }
            return overrideMap;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        catch ( SQLException se )
        {
            throw new SqlException(se);
        }
    }

    private int buildPlaceholderLength(long totalLength) {
        if (totalLength < 0x7fff) {
            return 0x8002; // need 2 bytes
        } else if (totalLength < 0x7fffffff) {
            return 0x8004; // need 4 bytes
        } else if (totalLength < 0x7fffffffffffL) {
            return 0x8006;
        } else {
            return 0x8008; // need 8 bytes
        }
    }

    // Output Expected indicates wheterh the requester expects the target
    // SQLAM to return output with an SQLDTARD reply data object
    // as a result of the execution of the referenced SQL statement.
    // this is a single byte.
    // there are two possible enumerated values:
    // 0x'F1' (CodePoint.TRUE) - for true indicating the requester expects output
    // 0x'F0' (CodePoint.FALSE) - for false indicating the requeser does not expect output
    // 0x'F0' is the default.
    //
    // preconditions:
    //   sqlam must support this parameter on the command, method will not check.
    private void buildOUTEXP(boolean outputExpected) throws SqlException {
        if (outputExpected) {
            writeScalar1Byte(CodePoint.OUTEXP, CodePoint.TRUE);
        }
    }

    // Maximum Number of Extra Blocks specifies a limit on the number of extra
    // blocks of answer set data per result set that the requester is capable of
    // receiveing.
    // this value must be able to be contained in a two byte signed number.
    // there is a minimum value of 0.
    // a zero indicates that the requester is not capable of receiving extra
    // query blocks of answer set data.
    // there is a SPCVAL of -1.
    // a value of -1 indicates that the requester is capable of receiving
    // the entire result set.
    //
    // preconditions:
    //   sqlam must support this parameter on the command, method will not check.
    void buildMAXBLKEXT(int maxNumOfExtraBlocks) throws SqlException {
        if (maxNumOfExtraBlocks != 0) {
            writeScalar2Bytes(CodePoint.MAXBLKEXT, maxNumOfExtraBlocks);
        }
    }

    // preconditions:
    void buildQRYROWSET(int fetchSize) throws SqlException {
        writeScalar4Bytes(CodePoint.QRYROWSET, fetchSize);
    }

    // The Procedure Name.
    // The default value of PRCNAM is the procedure name value contained
    // within the section identified by the pkgnamcsn parameter.  If that
    // value is null, then the prcnam parameter must be specified.
    // it has a max length of 255.
    // the prcnam is required on commands if the procedure name is
    // specified by a host variable.
    // the default value is the procedure name contained in the section
    // specified by the pkgnamcsn parameter on the EXCSQLSTT command.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    //   prcnam can not be null, SQLException will be thrown
    //   prcnam can not be 0 length or > 255 length, SQLException will be thrown.
    private void buildPRCNAM(String prcnam) throws SqlException {
        if (prcnam == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-848
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_NULL_PROCEDURE_NAME));
        }

        int prcnamLength = prcnam.length();
        if ((prcnamLength == 0) || (prcnamLength > 255)) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_PROCEDURE_NAME_LENGTH_OUT_OF_RANGE),
//IC see: https://issues.apache.org/jira/browse/DERBY-5873
                prcnamLength, 255);
        }

        writeScalarString(CodePoint.PRCNAM, prcnam);
    }


    // Query Block Size specifies the query block size for the reply
    // data objects and the reply messages being returned from this command.
    // this is a 4 byte unsigned binary number.
    // the sqlam 6 min value is 512 and max value is 32767.
    // this value was increased in later sqlam levels.
    // until the code is ready to support larger query block sizes,
    // it will always use DssConstants.MAX_DSS_LEN which is 32767.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    void buildQRYBLKSZ() throws SqlException {
        writeScalar4Bytes(CodePoint.QRYBLKSZ, DssConstants.MAX_DSS_LEN);
    }

    // Maximum Result Set Count specifies a limit on the number of result sets
    // the requester is capable of receiving as reply data in response to an ECSQLSTT
    // command that invokes a stored procedure.  If the stored procedure generates
    // more than MAXRSLCNT result sets, then the target system returns at most, the first
    // MAXRSLCNT of these result sets.  The stored procedure defines the order
    // in which the target system returns result sets.
    // this is s two byte signed binary number.
    // it has a min value of 0 which indicates the requester is not capable
    // of receiving result sets as reply data in response to the command.
    // a special value, -1 (CodePoint.MAXRSLCNT_NOLIMIT = 0xffff), indicates the
    // requester is capable of receiving all result sets in response the EXCSQLSTT.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    //   the value must be in correct range (-1 to 32767), method will not check.
    private void buildMAXRSLCNT(int maxResultSetCount) throws SqlException {
        if (maxResultSetCount == 0) {
            return;
        }
        writeScalar2Bytes(CodePoint.MAXRSLCNT, maxResultSetCount);
    }

    // RDB Commit Allowed specifies whether an RDB should allow the processing of any
    // commit or rollback operations that occure during execution of a statement.
    // True allow the processing of commits and rollbacks
    private void buildRDBCMTOK() throws SqlException {
        writeScalar1Byte(CodePoint.RDBCMTOK, CodePoint.TRUE);
    }

    // Result Set Flags is a single byte where each bit it a boolean flag.
    // It specifies wheter the requester desires the server to return name,
    // label and comment information for the columns of result sets generated by the command.
    // The default is b'00000000'.
    // columnNamesRequired
    //    false means the requester does not desire column names returned.
    //    true means the requester desires column names returned.
    // columnLabelsRequired
    //    false means the requester does not desire column labels returned.
    //    true means the requester desires column labels returned.
    // columnCommentsRequired
    //    false means the requester does not desire column comments returned.
    //    true means the requester desired column comments returned.
    // cantProcessAnswerSetData
    //    false means that for each result set, the requester expects the command
    //    to return an FDOCA description of the answer set data and to possibly
    //    return answer set data.  the block containing the end of the description
    //    may be completed if room exists with answer set data.  additional blocks
    //    of answer set data may also be chained to the block containing the end of the
    //    FDOCA description.  up to the maximum number of extra blocks of answer set data
    //    per result set specified in the MAXBLKEXT parameter.
    //    true means that for each result set, the requester expects the command to return
    //    an FDOCA description of the answer set data but does not expect the command to
    //    return any answer set data.
    // at SQLAM 7, new flags are supported which can be used to return
    // standard, extended, and light sqlda
    //
    // preconditions:
    //    sqlam must support this parameter, method will not check.
    private void buildRSLSETFLG(int resultSetFlag) throws SqlException {
        writeScalar1Byte(CodePoint.RSLSETFLG, resultSetFlag);
    }

    void buildQRYINSID(long qryinsid) throws SqlException {
        markLengthBytes(CodePoint.QRYINSID);
        writeLong(qryinsid);
        updateLengthBytes();
    }


    // Return SQL Descriptor Area controls whether to return
    // an SQL descriptor area that applies to the SQL statement this command
    // identifies.  The target SQLAM obtains the SQL descriptor area by performing
    // an SQL DESCRIBE function on the statement after the statement has been
    // prepared.
    // The value TRUE, X'F1' (CodePoint.TRUE), indicates an SQLDA is returned
    // The value FALSE, X'F0' (CodePoint.FALSE), default, indicates an SQLDA is not returned.
    //
    // preconditions:
    //   sqlam must support this parameter for the command, method will not check.
    private void buildRTNSQLDA() throws SqlException {
        writeScalar1Byte(CodePoint.RTNSQLDA, CodePoint.TRUE);
    }

    // Type of SQL Descriptor Area.
    // This is a single byte signed number that specifies the type of sqlda to
    // return for the command.
    // below sqlam 7 there were two possible enumerated values for this parameter.
    // 0 (CodePoint.TYPSQLDA_STD_OUTPUT)- the default, indicates return the output sqlda.
    // 1 (CodePoint.TYPSQLDA_STD_INPUT) - indicates return the input sqlda.
    // the typsqlda was enhanced at sqlam 7 to support extened describe.
    // at sqlam 7 the following enumerated values are supported.
    // 0 (CodePoint.TYPSQLDA_STD_OUTPUT) - the default, standard output sqlda.
    // 1 (CodePoint.TYPSQLDA_STD_INPUT) - standard input sqlda.
    // 2 (CodePoint.TYPSQLDA_LIGHT_OUTPUT) - light output sqlda.
    // 3 (CodePoint.TYPSQLDA_LIGHT_INPUT) - light input sqlda.
    // 4 (CodePoint.TYPSQLDA_X_OUTPUT) - extended output sqlda.
    // 5 (CodePoint.TYPSQLDA_X_INPUT) - extended input sqlda.
    //
    // preconditions:
    //   sqlam or prdid must support this, method will not check.
    //   valid enumerated type must be passed to method, method will not check.
    private void buildTYPSQLDA(int typeSqlda) throws SqlException {
        // possibly inspect typeSqlda value and verify against sqlam level
        if (typeSqlda != CodePoint.TYPSQLDA_STD_OUTPUT) {
            writeScalar1Byte(CodePoint.TYPSQLDA, typeSqlda);
        }
    }

    /**
     * Build QRYCLSIMP (Query Close Implicit). Query Close Implicit
     * controls whether the target server implicitly closes a
     * non-scrollable query upon end of data (SQLSTATE 02000).
     */
    private void buildQRYCLSIMP() {
//IC see: https://issues.apache.org/jira/browse/DERBY-821
        writeScalar1Byte(CodePoint.QRYCLSIMP, CodePoint.QRYCLSIMP_YES);
    }

    // helper method to buildFDODTA to build the actual data length
    private void setFDODTALobLength(int[][] protocolTypesAndLengths, int i, long dataLength) throws SqlException {
        if (protocolTypesAndLengths[i][1] == 0x8002) {
            writeShort((short) dataLength);
        } else if (protocolTypesAndLengths[i][1] == 0x8004) {
            writeInt((int) dataLength);  // 4 bytes to encode the length
//IC see: https://issues.apache.org/jira/browse/DERBY-1595
        } else if (protocolTypesAndLengths[i][1] == 0x8006) {
            writeLong6Bytes(dataLength); // 6 bytes to encode the length
        } else if (protocolTypesAndLengths[i][1] == 0x8008) {
            writeLong(dataLength); // 8 bytes to encode the length
        }

        if (dataLength != 0) {
            if (extdtaPositions_ == null) {
                extdtaPositions_ = new ArrayList<Integer>();
            }
            extdtaPositions_.add(i);
        }
    }
    
    private void setFDODTALobLengthUnknown(int i) throws SqlException {
        short v = 1;
        writeShort( v <<= 15 );
        if (extdtaPositions_ == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            extdtaPositions_ = new ArrayList<Integer>();
        }
        
        extdtaPositions_.add(i);
    }

    private boolean checkSendQryrowset(int fetchSize,
                                       int resultSetType) {
        // if the cursor is forward_only, ignore the fetchSize and let the server return
        // as many rows as fit in the query block.
        // if the cursor is scrollable, send qryrowset if it is supported by the server
        boolean sendQryrowset = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            sendQryrowset = true;
        }
        return sendQryrowset;
    }

    private int checkFetchsize(int fetchSize, int resultSetType) {
        // if fetchSize is not set for scrollable cursors, set it to the default fetchSize
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY && fetchSize == 0) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            fetchSize = Configuration.defaultFetchSize;
        }
        return fetchSize;
    }

    private int calculateResultSetFlags() {
        return CodePoint.RSLSETFLG_EXTENDED_SQLDA;
    }

    public void writeSetSpecialRegister(Section section, ArrayList sqlsttList)
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
            throws SqlException {
        buildEXCSQLSET(section);

        // SQLSTT:
        for (int i = 0; i < sqlsttList.size(); i++) {
            buildSQLSTTcommandData((String) sqlsttList.get(i));
        }
    }

    private int[][] allocateLidAndLengthsArray(ColumnMetaData parameterMetaData) {
        int numVars = parameterMetaData.columns_;
        int[][] lidAndLengths = parameterMetaData.protocolTypesCache_;
        if ((lidAndLengths) == null || (lidAndLengths.length != numVars)) {
            lidAndLengths = new int[numVars][2];
            parameterMetaData.protocolTypesCache_ = lidAndLengths;
        }
        return lidAndLengths;
    }

    private void buildMddOverrides(ArrayList sdaOverrides) throws SqlException {
        byte[] mddBytes;
        for (int i = 0; i < sdaOverrides.size(); i++) {
            mddBytes = (byte[]) (sdaOverrides.get(i));
            writeBytes(mddBytes);
        }
    }

    private void setFDODTALob(int securityMechanism,
                              Lob lob,
                              int[][] protocolTypesAndLengths,
                              int i) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        throws SqlException, SQLException{
        
        if( lob.willBeLayerBStreamed() ) {
            
            setFDODTALobLengthUnknown( i );
            
        } else {
            setFDODTALobLength(protocolTypesAndLengths, 
                               i, 
                               lob.length() );
        }
        
    }
    
    
}


