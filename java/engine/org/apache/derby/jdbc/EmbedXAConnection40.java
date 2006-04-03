/*
 
   Derby - class org.apache.derby.jdbc.EmbedXAConnection40
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
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


package org.apache.derby.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.apache.derby.iapi.jdbc.EngineConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.impl.jdbc.Util;

/**
 * This class implements jdbc4.0 methods of XAConnection
 * It inherits these methods from EmbedPooledConnection40
 */
final class EmbedXAConnection40 extends EmbedPooledConnection40
        implements XAConnection {
    private EmbedXAResource xaRes;
    
    /**
     * Creates a new instance of EmbedXAConnection40. Initializes XAResource for
     * the connection
     * @param ds
     * @param ra
     * @param user
     * @param password
     * @param requestPassword
     */
    EmbedXAConnection40(EmbeddedDataSource40 ds, ResourceAdapter ra,
            String user, String password,
            boolean requestPassword) throws SQLException {
        super(ds, user, password, requestPassword);
        xaRes = new EmbedXAResource(this, ra);
    }
    
    /**
     * Retrieves an <code>XAResource</code> object that
     * the transaction manager will use
     * to manage this <code>XAConnection</code> object's participation in a
     * distributed transaction.
     *
     * @return the <code>XAResource</code> object
     * @exception SQLException if a database access error occurs
     */
    public final synchronized XAResource getXAResource() throws SQLException {
        checkActive ();
        return xaRes;
    }
    
    /**
     * Allow control over setting auto commit mode.
     * @param autoCommit 
     */
    public void checkAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit && (xaRes.getCurrentXid() != null))
            throw Util.generateCsSQLException(SQLState.CANNOT_AUTOCOMMIT_XA);
        
        super.checkAutoCommit(autoCommit);
    }

    /**
     * Are held cursors allowed. If the connection is attached to
     * a global transaction then downgrade the result set holdabilty
     * to CLOSE_CURSORS_AT_COMMIT if downgrade is true, otherwise
     * throw an exception.
     * If the connection is in a local transaction then the
     * passed in holdabilty is returned.
     * @param holdability 
     * @param downgrade 
     * @return holdability
     */
    public int checkHoldCursors(int holdability, boolean downgrade)
                                                        throws SQLException {
        if (holdability == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {
            if (xaRes.getCurrentXid() != null) {
                if (downgrade)
                    return JDBC30Translation.CLOSE_CURSORS_AT_COMMIT;
                throw Util.generateCsSQLException(SQLState.CANNOT_HOLD_CURSOR_XA);
            }
        }
        
        return super.checkHoldCursors(holdability, downgrade);
    }
        
    /**
     * Allow control over creating a Savepoint (JDBC 3.0)
     */
    public void checkSavepoint() throws SQLException {
        
        if (xaRes.getCurrentXid() != null)
            throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);
        
        super.checkSavepoint();
    }
    
    /**
     * Allow control over calling rollback.
     */
    public void checkRollback() throws SQLException {
        
        if (xaRes.getCurrentXid() != null)
            throw Util.generateCsSQLException(SQLState.CANNOT_ROLLBACK_XA);
        
        super.checkRollback();
    }
    /**
     * Allow control over calling commit.
     */
    public void checkCommit() throws SQLException {
        
        if (xaRes.getCurrentXid() != null)
            throw Util.generateCsSQLException(SQLState.CANNOT_COMMIT_XA);
        
        super.checkCommit();
    }
    
    /**
     * Create an object handle for a database connection.
     * @return a Connection object
     * @exception SQLException - if a database-access error occurs.
     */
    public Connection getConnection() throws SQLException {
        Connection handle;
        
        // Is this just a local transaction?
        if (xaRes.getCurrentXid() == null) {
            handle = super.getConnection();
        } else {
            
            if (currentConnectionHandle != null) {
                // this can only happen if someone called start(Xid),
                // getConnection, getConnection (and we are now the 2nd
                // getConnection call).
                // Cannot yank a global connection away like, I don't think...
                throw Util.generateCsSQLException(
                        SQLState.CANNOT_CLOSE_ACTIVE_XA_CONNECTION);
            }
            
            handle = getNewCurrentConnectionHandle();
        }
        
        currentConnectionHandle.syncState();
        
        return handle;
    }
    
    /**
     * Wrap and control a Statement
     * @param s 
     * @return Statement
     */
    public Statement wrapStatement(Statement s) throws SQLException {
        XAStatementControl sc = new XAStatementControl(this, s);
        return sc.applicationStatement;
    }
    
    /**
     * Wrap and control a PreparedStatement
     * @param ps 
     * @param sql 
     * @param generatedKeys 
     * @return PreparedStatement
     */
    public PreparedStatement wrapStatement(PreparedStatement ps, String sql, 
                                    Object generatedKeys) throws SQLException {
        XAStatementControl sc = new XAStatementControl(this, ps, 
                                                        sql, generatedKeys);
        return (PreparedStatement) sc.applicationStatement;
    }
    
    /**
     * Wrap and control a PreparedStatement
     * @param cs 
     * @param sql 
     * @return CallableStatement
     */
    public CallableStatement wrapStatement(CallableStatement cs, 
                                        String sql) throws SQLException {
        XAStatementControl sc = new XAStatementControl(this, cs, sql);
        return (CallableStatement) sc.applicationStatement;
    }
    
    /**
     * Override getRealConnection to create a a local connection
     * when we are not associated with an XA transaction.
     *
     * This can occur if the application has a Connection object (conn)
     * and the following sequence occurs.
     *
     * conn = xac.getConnection();
     * xac.start(xid, ...)
     *
     * // do work with conn
     *
     * xac.end(xid, ...);
     *
     * // do local work with conn
     * // need to create new connection here.
     */
    public EngineConnection getRealConnection() throws SQLException {
        EngineConnection rc = super.getRealConnection();
        if (rc != null)
            return rc;
        
        openRealConnection();
        
        // a new Connection, set its state according to the application's 
        //Connection handle
        currentConnectionHandle.setState(true);        
        return realConnection;
    }
    
    
    
    
}
