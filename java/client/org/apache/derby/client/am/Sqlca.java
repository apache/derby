/*

   Derby - Class org.apache.derby.client.am.Sqlca

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

package org.apache.derby.client.am;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.client.net.Typdef;

public abstract class Sqlca {
    transient protected Connection connection_;
    SqlException exceptionThrownOnStoredProcInvocation_;
    boolean messageTextRetrievedContainsTokensOnly_ = true;

    // data corresponding to SQLCA fields
    protected int sqlCode_;        // SQLCODE
    private String sqlErrmc_;       // A string with all error tokens delimited by sqlErrmcDelimiter
    protected String[] sqlErrmcTokens_; // A string array with each element
    // contain an error token
    protected String sqlErrp_;        // function name issuing error
    protected int[] sqlErrd_;        // 6 diagnostic Information
    protected char[] sqlWarn_;        // 11 warning Flags
    protected String sqlState_;       // SQLSTATE

    // raw sqlca data fields before unicode conversion
    protected byte[] sqlErrmcBytes_;
    protected byte[] sqlErrpBytes_;
    protected byte[] sqlWarnBytes_;
    protected byte[] sqlStateBytes_;

    protected int ccsid_;
    protected int sqlErrmcCcsid_;
    protected boolean containsSqlcax_ = true;
    protected long rowsetRowCount_;

    //public static final String sqlErrmcDelimiter = "\u00FF";
    private static final String sqlErrmcDelimiter__ = ";";

    // JDK stack trace calls e.getMessage(), so we must set some state on the sqlca that says return tokens only.
    private boolean returnTokensOnlyInMessageText_ = false;

    transient private final Agent agent_;

    private String cachedMessage;

    protected Sqlca(org.apache.derby.client.am.Connection connection) {
        connection_ = connection;
        agent_ = connection_ != null ? connection_.agent_ : null;
    }

    void returnTokensOnlyInMessageText(boolean returnTokensOnlyInMessageText) {
        returnTokensOnlyInMessageText_ = returnTokensOnlyInMessageText;
    }

    synchronized public int getSqlCode() {
        return sqlCode_;
    }

    synchronized public String getSqlErrmc() {
        if (sqlErrmc_ != null) {
            return sqlErrmc_;
        }

        // sqlErrmc string is dependent on sqlErrmcTokens array having been built
        if (sqlErrmcTokens_ == null) {
            getSqlErrmcTokens();
        }

        // sqlErrmc will be build only if sqlErrmcTokens has been build.
        // Otherwise, a null string will be returned.
        if (sqlErrmcTokens_ == null) {
            return null;
        }

        // create 0-length String if no tokens
        if (sqlErrmcTokens_.length == 0) {
            sqlErrmc_ = "";
            return sqlErrmc_;
        }

        // concatenate tokens with sqlErrmcDelimiter delimiters into one String
        StringBuffer buffer = new StringBuffer();
        int indx;
        for (indx = 0; indx < sqlErrmcTokens_.length - 1; indx++) {
            buffer.append(sqlErrmcTokens_[indx]);
            buffer.append(sqlErrmcDelimiter__);
        }
        // add the last token
        buffer.append(sqlErrmcTokens_[indx]);

        // save as a string
        sqlErrmc_ = buffer.toString();
        return sqlErrmc_;
    }

    synchronized public String[] getSqlErrmcTokens() {
        if (sqlErrmcTokens_ != null) {
            return sqlErrmcTokens_;
        }

        // processSqlErrmcTokens handles null sqlErrmcBytes_ case
        sqlErrmcTokens_ = processSqlErrmcTokens(sqlErrmcBytes_);
        return sqlErrmcTokens_;
    }

    synchronized public String getSqlErrp() {
        if (sqlErrp_ != null) {
            return sqlErrp_;
        }

        if (sqlErrpBytes_ == null) {
            return null;
        }

        try {
            sqlErrp_ = bytes2String(sqlErrpBytes_,
                    0,
                    sqlErrpBytes_.length);
            return sqlErrp_;
        } catch (java.io.UnsupportedEncodingException e) {
            // leave sqlErrp as null.
            return null;
        }
    }

    public int[] getSqlErrd() {
        if (sqlErrd_ != null) {
            return sqlErrd_;
        }

        sqlErrd_ = new int[6]; // create an int array.
        return sqlErrd_;
    }

    synchronized public char[] getSqlWarn() {
        if (sqlWarn_ != null) {
            return sqlWarn_;
        }

        try {
            if (sqlWarnBytes_ == null) {
                sqlWarn_ = new char[]{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}; // 11 blank.
            } else {
                sqlWarn_ = bytes2String(sqlWarnBytes_, 0, sqlWarnBytes_.length).toCharArray();
            }
            return sqlWarn_;
        } catch (java.io.UnsupportedEncodingException e) {
            sqlWarn_ = new char[]{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}; // 11 blank.
            return sqlWarn_;
        }
    }

    synchronized public String getSqlState() {
        if (sqlState_ != null) {
            return sqlState_;
        }

        if (sqlStateBytes_ == null) {
            return null;
        }

        try {
            sqlState_ = bytes2String(sqlStateBytes_,
                    0,
                    sqlStateBytes_.length);
            return sqlState_;
        } catch (java.io.UnsupportedEncodingException e) {
            // leave sqlState as null.
            return null;
        }
    }

    // Gets the formatted message, can throw an exception.
    synchronized public String getMessage() throws SqlException {
        // should this be traced to see if we are calling a stored proc?
        if (cachedMessage != null) {
            return cachedMessage;
        }

        if (connection_ == null || connection_.isClosedX() || returnTokensOnlyInMessageText_) {
            return getUnformattedMessage();
        }

        CallableStatement cs = null;
        synchronized (connection_) {
            try {
                cs = connection_.prepareMessageProc("call SYSIBM.SQLCAMESSAGE(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                // SQLCode: SQL return code.
                cs.setIntX(1, getSqlCode());
                // SQLErrml: Length of SQL error message tokens.
                cs.setShortX(2, (short) ((getSqlErrmc() != null) ? getSqlErrmc().length() : 0));
                // SQLErrmc: SQL error message tokens as a String (delimited by semicolon ";").
                cs.setStringX(3, getSqlErrmc());
                // SQLErrp: Product signature.
                cs.setStringX(4, getSqlErrp());
                // SQLErrd: SQL internal error code.
                cs.setIntX(5, getSqlErrd()[0]);
                cs.setIntX(6, getSqlErrd()[1]);
                cs.setIntX(7, getSqlErrd()[2]);
                cs.setIntX(8, getSqlErrd()[3]);
                cs.setIntX(9, getSqlErrd()[4]);
                cs.setIntX(10, getSqlErrd()[5]);
                // SQLWarn: SQL warning flags.
                cs.setStringX(11, new String(getSqlWarn()));
                // SQLState: standard SQL state.
                cs.setStringX(12, getSqlState());
                // MessageFileName: Not used by our driver, so set to null.
                cs.setStringX(13, null);
                // Locale: language preference requested for the return error message.
                cs.setStringX(14, java.util.Locale.getDefault().toString());
                // server could return a locale different from what we requested
                cs.registerOutParameterX(14, java.sql.Types.VARCHAR);
                // Message: error message returned from SQLCAMessage stored procedure.
                cs.registerOutParameterX(15, java.sql.Types.LONGVARCHAR);
                // RCode: return code from SQLCAMessage stored procedure.
                cs.registerOutParameterX(16, java.sql.Types.INTEGER);
                cs.executeX();

                if (cs.getIntX(16) == 0) {
                    // Return the message text.
                    messageTextRetrievedContainsTokensOnly_ = false;
                    String message = cs.getStringX(15);
                    cachedMessage = message;
                    return message;
                } else {
                    // Stored procedure can't return a valid message text, so we return
                    // unformated exception
                    return getUnformattedMessage();
                }
            } finally {
                if (cs != null) {
                    try {
                        cs.closeX();
                    } catch (SqlException doNothing) {
                    }
                }
            }
        }
    }

    // May or may not get the formatted message depending upon datasource directives.  cannot throw exeption.
    public synchronized String getJDBCMessage() {
        // The transient connection_ member will only be null if the Sqlca has been deserialized
        if (connection_ != null && connection_.retrieveMessageText_) {
            try {
                return getMessage();
            } catch (SqlException e) {
                // Invocation of stored procedure fails, so we return error message tokens directly.
                exceptionThrownOnStoredProcInvocation_ = e;
                chainDeferredExceptionsToAgentOrAsConnectionWarnings((SqlException) e);
                return getUnformattedMessage();
            }
        } else {
            return getUnformattedMessage();
        }
    }

    private String getUnformattedMessage() {
        return "DERBY SQL error: SQLCODE: " + getSqlCode() + ", SQLSTATE: " + getSqlState() + ", SQLERRMC: " + getSqlErrmc();
    }

    private void chainDeferredExceptionsToAgentOrAsConnectionWarnings(SqlException e) {
        SqlException current = e;
        while (current != null) {
            SqlException next = (SqlException) current.getNextException();
            current = current.copyAsUnchainedSQLException(agent_.logWriter_);
            if (current.getErrorCode() == -440) {
                SqlWarning warningForStoredProcFailure = new SqlWarning(agent_.logWriter_,
                    new ClientMessageId(SQLState.UNABLE_TO_OBTAIN_MESSAGE_TEXT_FROM_SERVER));
                warningForStoredProcFailure.setNextException(current.getSQLException());
                connection_.accumulate440WarningForMessageProcFailure(warningForStoredProcFailure);
            } else if (current.getErrorCode() == -444) {
                SqlWarning warningForStoredProcFailure = new SqlWarning(agent_.logWriter_,
                    new ClientMessageId(SQLState.UNABLE_TO_OBTAIN_MESSAGE_TEXT_FROM_SERVER));
                warningForStoredProcFailure.setNextException(current.getSQLException());
                connection_.accumulate444WarningForMessageProcFailure(warningForStoredProcFailure);
            } else {
                agent_.accumulateDeferredException(current);
            }
            current = next;
        }
    }

    public boolean includesSqlCode(int[] codes) {
        for (int i = 0; i < codes.length; i++) {
            if (codes[i] == getSqlCode()) {
                return true;
            }
        }
        return false;
    }
    // ------------------- helper methods ----------------------------------------

    private String[] processSqlErrmcTokens(byte[] tokenBytes) {
        if (tokenBytes == null) {
            return null;
        }

        // create 0-length String tokens array if tokenBytes is 0-length
        int length = tokenBytes.length;
        if (length == 0) {
            return new String[0];
        }

        try {
            // tokenize and convert tokenBytes
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
            java.util.LinkedList tokens = new java.util.LinkedList();

            // parse the error message tokens
            for (int index = 0; index < length - 1; index++) {

                // non-delimiter - continue to write into buffer
                if (tokenBytes[index] != -1)  // -1 is the delimiter '\xFF'
                {
                    buffer.write(tokenBytes[index]);
                }

                // delimiter - convert current token and add to list
                else {
                    tokens.add(bytes2String(buffer.toByteArray(), 0, buffer.size()));
                    buffer.reset();
                }
            }

            int lastIndex = length - 1;
            // check for last byte not being a delimiter, i.e. part of last token
            if (tokenBytes[lastIndex] != -1) {
                // write the last byte
                buffer.write(tokenBytes[lastIndex]);
                // convert the last token and add to list
                tokens.add(bytes2String(buffer.toByteArray(), 0, buffer.size()));
            }

            // last byte is delimiter implying an empty String for last token
            else {
                // convert current token, if one exists, and add to list
                if (lastIndex != 0) {
                    tokens.add(bytes2String(buffer.toByteArray(), 0, buffer.size()));
                }
                // last token is an empty String
                tokens.add("");
            }

            // create the String array and fill it with tokens.
            String[] tokenStrings = new String[tokens.size()];

            java.util.Iterator iterator = tokens.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                tokenStrings[i] = (String) iterator.next();
            }

            return tokenStrings;
        } catch (java.io.UnsupportedEncodingException e) {
            return null;
        }
    }

    private String bytes2String(byte[] bytes, int offset, int length)
            throws java.io.UnsupportedEncodingException {
        // Network server uses utf8 encoding
        return new String(bytes, offset, length, Typdef.UTF8ENCODING);
    }

    public int getUpdateCount() {
        if (sqlErrd_ == null) {
            return 0;
        }
        return sqlErrd_[2];
    }

    public long getRowCount() throws org.apache.derby.client.am.DisconnectException {
        return ((long) sqlErrd_[0] << 32) + sqlErrd_[1];
    }

    public void setContainsSqlcax(boolean containsSqlcax) {
        containsSqlcax_ = containsSqlcax;
    }

    public boolean containsSqlcax() {
        return containsSqlcax_;
    }

    public void resetRowsetSqlca(org.apache.derby.client.am.Connection connection,
                                 int sqlCode,
                                 byte[] sqlStateBytes,
                                 byte[] sqlErrpBytes,
                                 int ccsid) {
        connection_ = connection;
        sqlCode_ = sqlCode;
        sqlStateBytes_ = sqlStateBytes;
        sqlErrpBytes_ = sqlErrpBytes;
        ccsid_ = ccsid;
    }

    public void setRowsetRowCount(long rowCount) {
        rowsetRowCount_ = rowCount;
    }

    public long getRowsetRowCount() {
        return rowsetRowCount_;
    }
}

