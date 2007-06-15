/*

   Derby - Class org.apache.derby.client.net.NetXAConnectionRequest

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

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.derby.client.am.SqlException;

public class NetXAConnectionRequest extends NetResultSetRequest {
    NetXAConnectionRequest(NetAgent netAgent, CcsidManager ccsidManager, int bufferSize) {
        super(netAgent, ccsidManager, bufferSize);
    }

    //----------------------------- entry points ---------------------------------


    //Build the SYNNCTL commit command
    public void writeLocalXACommit(NetConnection conn) throws SqlException {
        NetXACallInfo callInfo =
                conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        Xid xid = callInfo.xid_;
        buildSYNCCTLMigrate();   // xa migrate to resync server
        buildSYNCCTLCommit(CodePoint.TMLOCAL, xid);   // xa local commit
    }

    //Build the SYNNCTL rollback command
    public void writeLocalXARollback(NetConnection conn) throws SqlException {
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        buildSYNCCTLRollback(CodePoint.TMLOCAL);   // xa local rollback
    }

    public void writeXaStartUnitOfWork(NetConnection conn) throws SqlException {
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        Xid xid = callInfo.xid_;
        int xaFlags = callInfo.xaFlags_;
        long xaTimeout = callInfo.xaTimeoutMillis_;

        // create DSS command with reply.
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_NEW_UOW);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);
        // Check whether the timeout value was specified.
        // Value less than 0 means no timeout is specified.
        if (xaTimeout >= 0) {
            writeXATimeout(CodePoint.TIMEOUT, xaTimeout);
        }
        updateLengthBytes();
    }

    public void writeXaEndUnitOfWork(NetConnection conn) throws SqlException {
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        Xid xid = callInfo.xid_;
        int xaFlags = callInfo.xaFlags_;

        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_END_UOW);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);

        updateLengthBytes();
    }

    protected void writeXaPrepare(NetConnection conn) throws SqlException {
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        Xid xid = callInfo.xid_;
        // don't forget that xars.prepare() does not have flags, assume TMNOFLAGS
        int xaFlags = XAResource.TMNOFLAGS;

        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_PREPARE);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);
        updateLengthBytes();
    }

    protected void writeXaCommit(NetConnection conn, Xid xid) throws SqlException {
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        int xaFlags = callInfo.xaFlags_;

        // create DSS command with no reply.
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_COMMITTED);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);
        updateLengthBytes();
    }

    protected void writeXaRollback(NetConnection conn, Xid xid) throws SqlException {
        int xaFlags = XAResource.TMNOFLAGS;

        // create DSS command with no reply.
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_ROLLBACK);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);
        updateLengthBytes();
    }

    protected void writeXaRecover(NetConnection conn, int flag) throws SqlException {
        // create DSS command with no reply.
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_INDOUBT);
        writeXAFlags(CodePoint.XAFLAGS, flag);
        updateLengthBytes();
    }

    protected void writeXaForget(NetConnection conn, Xid xid) throws SqlException {

        // create DSS command with no reply.
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_REQ_FORGET);

        writeXID(CodePoint.XID, xid);

        updateLengthBytes();
    }

    public void writeSYNCType(int codepoint, int syncType) {
        writeScalar1Byte(codepoint, syncType);
    }

    public void writeForget(int codepoint, int value) {
        writeScalar1Byte(codepoint, value);
    }

    public void writeReleaseConversation(int codepoint, int value) {
        writeScalar1Byte(codepoint, value);
    }

    void writeNullXID(int codepoint) {
        int nullXID = -1;
        writeScalar4Bytes(codepoint, nullXID);
    }

    void writeXID(int codepoint, Xid xid) throws SqlException {
        int len = 0;
        int formatId = xid.getFormatId();
        byte[] gtrid = xid.getGlobalTransactionId();
        byte[] bqual = xid.getBranchQualifier();

        markLengthBytes(codepoint);

        len = 4;                    // length of formatId
        len += (bqual.length + 4);  // bqual length
        len += (gtrid.length + 4);  // gtrid length

        write4Bytes(formatId);
        write4Bytes(gtrid.length);
        write4Bytes(bqual.length);

        // Mare sure request buffer has enough space to write this byte array.
        ensureLength(offset_ + gtrid.length);
        System.arraycopy(gtrid, 0, bytes_, offset_, gtrid.length);
        offset_ += gtrid.length;

        ensureLength(offset_ + bqual.length);
        System.arraycopy(bqual, 0, bytes_, offset_, bqual.length);
        offset_ += bqual.length;

        updateLengthBytes();

    }


    void writeXAFlags(int codepoint, int xaFlags) {
        writeScalar4Bytes(codepoint, xaFlags);
    }


    void writeXATimeout(int codepoint, long xaTimeout) {
        writeScalar8Bytes(codepoint, xaTimeout);
    }


    //----------------------helper methods----------------------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.





    void buildSYNCCTLMigrate() throws SqlException {
    }

    void buildSYNCCTLCommit(int xaFlags, Xid xid) throws SqlException {
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_COMMITTED);

        if (xid.getFormatId() != -1) {
            writeXID(CodePoint.XID, xid);
        } else
        // write the null XID for local transaction on XA connection
        {
            writeNullXID(CodePoint.XID);
        }

        writeXAFlags(CodePoint.XAFLAGS, xaFlags);

        updateLengthBytes();
    }

    void buildSYNCCTLRollback(int xaFlags) throws SqlException {
        createCommand();

        // save the length bytes for later update
        markLengthBytes(CodePoint.SYNCCTL);

        // SYNCTYPE
        writeSYNCType(CodePoint.SYNCTYPE, CodePoint.SYNCTYPE_ROLLBACK);

        // write the null XID for local transaction on XA connection
        writeNullXID(CodePoint.XID);
        writeXAFlags(CodePoint.XAFLAGS, xaFlags);
        updateLengthBytes();
    }

}



