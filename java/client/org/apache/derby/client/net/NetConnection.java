/*

   Derby - Class org.apache.derby.client.net.NetConnection

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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Array;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.transaction.xa.Xid;
import org.apache.derby.client.am.ClientCallableStatement;
import org.apache.derby.client.am.ClientDatabaseMetaData;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.EncryptionManager;
import org.apache.derby.client.am.ClientPreparedStatement;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.client.am.ClientStatement;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.ClientPooledConnection;
import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.ClientConnection;
import org.apache.derby.client.am.FailedProperties40;
import org.apache.derby.client.am.LogWriter;
import org.apache.derby.client.am.SQLExceptionFactory;
import org.apache.derby.client.am.Section;
import org.apache.derby.client.am.SectionManager;
import org.apache.derby.jdbc.BasicClientDataSource40;
import org.apache.derby.jdbc.ClientDataSourceInterface;

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.sanity.SanityManager;

public class NetConnection extends ClientConnection {
    
    // Use this to get internationalized strings...
    private static final MessageUtil msgutil = SqlException.getMessageUtil();

    protected NetAgent netAgent_;
    //contains a reference to the PooledConnection from which this created 
    //It then passes this reference to the PreparedStatement created from it
    //The PreparedStatement then uses this to pass the close and the error
    //occurred conditions back to the PooledConnection which can then throw the 
    //appropriate events.
    private final ClientPooledConnection pooledConnection_;
    private final boolean closeStatementsOnClose;

    //-----------------------------state------------------------------------------

    // these variables store the manager levels for the connection.
    // they are initialized to the highest value which this driver supports
    // at the current time.  theses intial values should be increased when
    // new manager level support is added to this driver.  these initial values
    // are sent to the server in the excsat command.  the server will return a
    // set of values and these will be parsed out by parseExcsatrd and parseMgrlvlls.
    // during this parsing, these instance variable values will be reset to the negotiated
    // levels for the connection.  these values may be less than the
    // values origionally set here at constructor time.  it is these new values
    // (following the parse) which are the levels for the connection.  after
    // a successful excsat command, these values can be checked to see
    // what protocol is supported by this particular connection.
    // if support for a new manager class is added, the buildExcsat and parseMgrlvlls
    // methods will need to be changed to accomodate sending and receiving the new class.
    protected int targetAgent_ = NetConfiguration.MGRLVL_7;  //01292003jev monitoring
    protected int targetCmntcpip_ = NetConfiguration.MGRLVL_5;
    protected int targetRdb_ = NetConfiguration.MGRLVL_7;
    int targetSecmgr_ = NetConfiguration.MGRLVL_7;
    protected int targetCmnappc_ = NetConfiguration.MGRLVL_NA;  //NA since currently not used by net
    protected int targetXamgr_ = NetConfiguration.MGRLVL_7;
    protected int targetSyncptmgr_ = NetConfiguration.MGRLVL_NA;
    protected int targetRsyncmgr_ = NetConfiguration.MGRLVL_NA;
    protected int targetUnicodemgr_ = CcsidManager.UTF8_CCSID;

    private String extnam_;

    // Server Class Name of the target server returned in excsatrd.
    // Again this is something which the driver is not currently using
    // to make any decions.  Right now it is just stored for future logging.
    // It does contain some useful information however and possibly
    // the database meta data object will make use of this
    // for example, the product id (prdid) would give this driver an idea of
    // what type of sevrer it is connected to.
    String targetSrvclsnm_;

    // Server Product Release Level of the target server returned in excsatrd.
    // specifies the procuct release level of a ddm server.
    // Again this is something which we don't currently use but
    // keep it in case we want to log it in some problem determination
    // trace/dump later.
    String targetSrvrlslv_;

    // Keys used for encryption.
    private transient byte[] publicKey_;
    private transient byte[] targetPublicKey_;

    // Seeds used for strong password substitute generation (USRSSBPWD)
    private transient byte[] sourceSeed_;   // Client seed
    private transient byte[] targetSeed_;   // Server seed

    // Product-Specific Data (prddta) sent to the server in the accrdb command.
    // The prddta has a specified format.  It is saved in case it is needed again
    // since it takes a little effort to compute.  Saving this information is
    // useful for when the connect flows need to be resent (right now the connect
    // flow is resent when this driver disconnects and reconnects with
    // non unicode ccsids.  this is done when the server doesn't recoginze the
    // unicode ccsids).
    //
    private ByteBuffer prddta_;

    // Correlation Token of the source sent to the server in the accrdb.
    // It is saved like the prddta in case it is needed for a connect reflow.
    byte[] crrtkn_;

    // The Secmec used by the target.
    // It contains the negotiated security mechanism for the connection.
    // Initially the value of this is 0.  It is set only when the server and
    // the target successfully negotiate a security mechanism.
    private int targetSecmec_;

    // the security mechanism requested by the application
    private int securityMechanism_;

    // stored the password for deferred reset only.
    private transient char[] deferredResetPassword_ = null;
    
    //If Network Server gets null connection from the embedded driver, 
    //it sends RDBAFLRM followed by SQLCARD with null SQLException.
    //Client will parse the SQLCARD and set connectionNull to true if the
    //SQLCARD is empty. If connectionNull=true, connect method in 
    //ClientDriver will in turn return null connection.
    private boolean connectionNull = false;

    private void setDeferredResetPassword(String password) {
        deferredResetPassword_ = (password == null) ? null : flipBits(password.toCharArray());
    }

    private String getDeferredResetPassword() {
        if (deferredResetPassword_ == null) {
            return null;
        }
        String password = new String(flipBits(deferredResetPassword_));
        flipBits(deferredResetPassword_); // re-encrypt password
        return password;
    }

    protected NetXAResource xares_ = null;
    private List<Xid> indoubtTransactions_ = null;
    protected int currXACallInfoOffset_ = 0;

    /**
     * Prepared statement that is used each time isValid() is called on this
     * connection. The statement is created the first time isValid is called
     * and closed when the connection is closed (by the close call).
     */
    private PreparedStatement isValidStmt;

    //---------------------constructors/finalizer---------------------------------

    // For jdbc 1 connections
    NetConnection(LogWriter logWriter,
                         int driverManagerLoginTimeout,
                         String serverName,
                         int portNumber,
                         String databaseName,
                         Properties properties) throws SqlException {
        super(logWriter, driverManagerLoginTimeout, serverName, portNumber,
              databaseName, properties);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        netAgent_ = (NetAgent) super.agent_;
        if (netAgent_.exceptionOpeningSocket_ != null) {
            throw netAgent_.exceptionOpeningSocket_;
        }
        checkDatabaseName();
        String password = BasicClientDataSource40.getPassword(properties);
        securityMechanism_ =
                BasicClientDataSource40.getSecurityMechanism(properties);
        flowConnect(password, securityMechanism_);
        if(!isConnectionNull())
            completeConnect();
        //DERBY-2026. reset timeout after connection is made
        netAgent_.setTimeout(0);
    }

    // For JDBC 2 Connections
    NetConnection(LogWriter logWriter,
                         String user,
                         String password,
                         BasicClientDataSource40 dataSource,
                         int rmId,
                         boolean isXAConn) throws SqlException {
        super(logWriter, user, password, isXAConn, dataSource);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        netAgent_ = (NetAgent) super.agent_;
        initialize(password, dataSource, isXAConn);
    }

    // For JDBC 2 Connections
    /**
     * This constructor is called from the ClientPooledConnection object 
     * to enable the NetConnection to pass <code>this</code> on to the associated 
     * prepared statement object thus enabling the prepared statement object 
     * to in turn raise the statement events to the ClientPooledConnection object
     * @param logWriter    LogWriter object associated with this connection
     * @param user         user id for this connection
     * @param password     password for this connection
     * @param dataSource   The DataSource object passed from the PooledConnection 
     *                     object from which this constructor was called
     * @param rmId         The Resource manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this 
     *                     NetConnection constructor was called. This is used
     *                     to pass StatementEvents back to the pooledConnection
     *                     object
     * @throws             SqlException
     */
    
    NetConnection(LogWriter logWriter,
                         String user,
                         String password,
                         BasicClientDataSource40 dataSource,
                         int rmId,
                         boolean isXAConn,
                         ClientPooledConnection cpc) throws SqlException {
        super(logWriter, user, password, isXAConn, dataSource);
        netAgent_ = (NetAgent) super.agent_;
        initialize(password, dataSource, isXAConn);
        this.pooledConnection_=cpc;
        this.closeStatementsOnClose = !cpc.isStatementPoolingEnabled();
    }

    private void initialize(String password,
                            BasicClientDataSource40 dataSource,
                            boolean isXAConn) throws SqlException {
        securityMechanism_ = dataSource.getSecurityMechanism(password);

        setDeferredResetPassword(password);
        checkDatabaseName();
        dataSource_ = dataSource;
        this.isXAConnection_ = isXAConn;
        flowConnect(password, securityMechanism_);
        // it's possible that the internal Driver.connect() calls returned null,
        // thus, a null connection, e.g. when the databasename has a : in it
        // (which the InternalDriver assumes means there's a subsubprotocol)  
        // and it's not a subsubprotocol recognized by our drivers.
        // If so, bail out here.
        if(!isConnectionNull()) {
            completeConnect();
        }
        else
        {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    new ClientMessageId(SQLState.PROPERTY_INVALID_VALUE),
                    Attribute.DBNAME_ATTR,databaseName_));
        }
        // DERBY-2026
        //reset timeout if previously set for login timeout
        netAgent_.setTimeout(0);
        
    }

    // preferably without password in the method signature.
    // We can probally get rid of flowReconnect method.
    private void resetNetConnection(LogWriter logWriter)
            throws SqlException {
        super.resetConnection(logWriter);
        //----------------------------------------------------
        // do not reset managers on a connection reset.  this information shouldn't
        // change and can be used to check secmec support.

        targetSrvclsnm_ = null;
        targetSrvrlslv_ = null;
        publicKey_ = null;
        targetPublicKey_ = null;
        sourceSeed_ = null;
        targetSeed_ = null;
        targetSecmec_ = 0;
        resetConnectionAtFirstSql_ = false;
        // properties prddta_ and crrtkn_ will be initialized by
        // calls to constructPrddta() and constructCrrtkn()
        //----------------------------------------------------------
        boolean isDeferredReset = flowReconnect(getDeferredResetPassword(),
                                                securityMechanism_);
        completeReset(isDeferredReset);
        //DERBY-2026. Make sure soTimeout is set back to
        // infinite after connection is made.
        netAgent_.setTimeout(0);
    }


    protected void reset_(LogWriter logWriter)
            throws SqlException {
        if (inUnitOfWork_) {
            throw new SqlException(logWriter, 
                new ClientMessageId(
                    SQLState.NET_CONNECTION_RESET_NOT_ALLOWED_IN_UNIT_OF_WORK));
        }
        resetNetConnection(logWriter);
    }

    private void completeReset(boolean isDeferredReset)
            throws SqlException {
        super.completeReset(isDeferredReset, closeStatementsOnClose, xares_);
    }

    private void flowConnect(String password,
                            int securityMechanism) throws SqlException {
        netAgent_ = (NetAgent) super.agent_;
        constructExtnam();
        // these calls need to be after newing up the agent
        // because they require the ccsid manager
        constructPrddta();  // construct product data

        netAgent_.typdef_ = new Typdef(netAgent_, 1208, NetConfiguration.SYSTEM_ASC, 1200, 1208);
        netAgent_.targetTypdef_ = new Typdef(netAgent_);
        netAgent_.originalTargetTypdef_ = netAgent_.targetTypdef_;
        setDeferredResetPassword(password);
        try {
            switch (securityMechanism) {
            case NetConfiguration.SECMEC_USRIDPWD: // Clear text user id and password
                checkUserPassword(user_, password);
                flowUSRIDPWDconnect(password);
                break;
            case NetConfiguration.SECMEC_USRIDONL: // Clear text user, no password sent to server
                checkUser(user_);
                flowUSRIDONLconnect();
                break;
            case NetConfiguration.SECMEC_USRENCPWD: // Clear text user, encrypted password
                checkUserPassword(user_, password);
                flowUSRENCPWDconnect(password);
                break;
            case NetConfiguration.SECMEC_EUSRIDPWD: // Encrypted user, encrypted password
                checkUserPassword(user_, password);
                flowEUSRIDPWDconnect(password);
                break;
            case NetConfiguration.SECMEC_EUSRIDDTA:
                checkUserPassword(user_, password);
                flowEUSRIDDTAconnect();
                break;
            case NetConfiguration.SECMEC_EUSRPWDDTA:
                checkUserPassword(user_, password);
                flowEUSRPWDDTAconnect(password);
                break;
            case NetConfiguration.SECMEC_USRSSBPWD: // Clear text user, strong password substitute
                checkUserPassword(user_, password);
                flowUSRSSBPWDconnect(password);
                break;

            default:
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.SECMECH_NOT_SUPPORTED),
                    securityMechanism);
            }
        } catch (Throwable e) {
            // If *anything* goes wrong, make sure the connection is
            // destroyed.
            // Always mark the connection closed in case of an error.
            // This prevents attempts to use this closed connection
            // to retrieve error message text if an error SQLCA
            // is returned in one of the connect flows.
            open_ = false;

            handleLoginTimeout( e );
            
            // logWriter may be closed in agent_.close(),
            // so SqlException needs to be created before that
            // but to be thrown after.
            SqlException exceptionToBeThrown;
            if (e instanceof SqlException) // rethrow original exception if it's an SqlException
            {
                exceptionToBeThrown = (SqlException) e;
            } else // any other exceptions will be wrapped by an SqlException first
            {
                exceptionToBeThrown = new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.JAVA_EXCEPTION),
                    e, e.getClass().getName(), e.getMessage());
            }

            try {
                if (agent_ != null) {
                    agent_.close();
                }
            } catch (SqlException ignoreMe) {
            }

            throw exceptionToBeThrown;
        }
    }
    
    private void flowSimpleConnect() throws SqlException {
        netAgent_ = (NetAgent) super.agent_;
        constructExtnam();
        // these calls need to be after newing up the agent
        // because they require the ccsid manager
        constructPrddta();  // construct product data

        netAgent_.typdef_ = new Typdef(netAgent_, 1208, NetConfiguration.SYSTEM_ASC, 1200, 1208);
        netAgent_.targetTypdef_ = new Typdef(netAgent_);
        netAgent_.originalTargetTypdef_ = netAgent_.targetTypdef_;

        try {
            flowServerAttributes();
        } catch (Throwable e) {
            // If *anything* goes wrong, make sure the connection is
            // destroyed.
            // Always mark the connection closed in case of an error.
            // This prevents attempts to use this closed connection
            // to retrieve error message text if an error SQLCA
            // is returned in one of the connect flows.
            open_ = false;

            handleLoginTimeout( e );
            
            // logWriter may be closed in agent_.close(),
            // so SqlException needs to be created before that
            // but to be thrown after.
            SqlException exceptionToBeThrown;
            if (e instanceof SqlException) // rethrow original exception if it's an SqlException
            {
                exceptionToBeThrown = (SqlException) e;
            } else // any other exceptions will be wrapped by an SqlException first
            {
                exceptionToBeThrown = new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.JAVA_EXCEPTION),
                    e, e.getClass().getName(), e.getMessage());
            }

            try {
                if (agent_ != null) {
                    agent_.close();
                }
            } catch (SqlException ignoreMe) {
            }

            throw exceptionToBeThrown;
        }
    }

    /** Handle socket timeouts during connection attempts */
    private void    handleLoginTimeout( Throwable original )
        throws SqlException
    {
        for ( Throwable cause = original; cause != null; cause = cause.getCause() )
        {
            if ( cause instanceof SocketTimeoutException )
            {
                throw new SqlException
                    ( agent_.logWriter_, new ClientMessageId( SQLState.LOGIN_TIMEOUT ), original );
            }
        }
    }

    private boolean flowReconnect(String password, int securityMechanism)
            throws SqlException {
        constructExtnam();
        // these calls need to be after newing up the agent
        // because they require the ccsid manager
        constructPrddta();  //modify this to not new up an array

        checkSecmgrForSecmecSupport(securityMechanism);
        try {
            switch (securityMechanism) {
            case NetConfiguration.SECMEC_USRIDPWD: // Clear text user id and password
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            case NetConfiguration.SECMEC_USRIDONL: // Clear text user, no password sent to server
                checkUser(user_);
                resetConnectionAtFirstSql_ = true;
                return true;
            case NetConfiguration.SECMEC_USRENCPWD: // Clear text user, encrypted password
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            case NetConfiguration.SECMEC_EUSRIDPWD: // Encrypted user, encrypted password
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            case NetConfiguration.SECMEC_EUSRIDDTA:
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            case NetConfiguration.SECMEC_EUSRPWDDTA:
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            case NetConfiguration.SECMEC_USRSSBPWD: // Clear text user, strong password substitute
                checkUserPassword(user_, password);
                resetConnectionAtFirstSql_ = true;
                setDeferredResetPassword(password);
                return true;
            default:
                throw new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.SECMECH_NOT_SUPPORTED),
                    securityMechanism);
            }
        } catch (SqlException sqle) {            // this may not be needed because on method up the stack
            open_ = false;                       // all reset exceptions are caught and wrapped in disconnect exceptions
            try {
                if (agent_ != null) {
                    agent_.close();
                }
            } catch (SqlException ignoreMe) {
            }
            throw sqle;
        }
    }

    //--------------------------------flow methods--------------------------------

    private void flowUSRIDPWDconnect(String password) throws SqlException {
        flowServerAttributesAndKeyExchange(NetConfiguration.SECMEC_USRIDPWD,
                null); // publicKey

        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                user_,
                password,
                null, //encryptedUserid
                null); //encryptedPassword
    }


    private void flowUSRIDONLconnect() throws SqlException {
        flowServerAttributesAndKeyExchange(NetConfiguration.SECMEC_USRIDONL,
                null); //publicKey

        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                user_,
                null, //password
                null, //encryptedUserid
                null); //encryptedPassword
    }


    private void flowUSRENCPWDconnect(String password) throws SqlException {
        flowServerAttributes();

        checkSecmgrForSecmecSupport(NetConfiguration.SECMEC_USRENCPWD);
        initializePublicKeyForEncryption();
        flowKeyExchange(NetConfiguration.SECMEC_USRENCPWD, publicKey_);

        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                user_,
                null, //password
                null, //encryptedUserid
                encryptedPasswordForUSRENCPWD(password));
    }


    private void flowEUSRIDPWDconnect(String password) throws SqlException {
        flowServerAttributes();

        checkSecmgrForSecmecSupport(NetConfiguration.SECMEC_EUSRIDPWD);
        initializePublicKeyForEncryption();
        flowKeyExchange(NetConfiguration.SECMEC_EUSRIDPWD, publicKey_);

        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                null, //user
                null, //password
                encryptedUseridForEUSRIDPWD(),
                encryptedPasswordForEUSRIDPWD(password));
    }

    private void flowEUSRIDDTAconnect() throws SqlException {
        flowServerAttributes();

        checkSecmgrForSecmecSupport(NetConfiguration.SECMEC_EUSRIDPWD);
        initializePublicKeyForEncryption();
        flowKeyExchange(NetConfiguration.SECMEC_EUSRIDDTA, publicKey_);


        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                null, //user
                null, //password
                encryptedUseridForEUSRIDPWD(),
                null);//encryptedPasswordForEUSRIDPWD (password),
    }

    private void flowEUSRPWDDTAconnect(String password) throws SqlException {
        flowServerAttributes();

        checkSecmgrForSecmecSupport(NetConfiguration.SECMEC_EUSRPWDDTA);
        initializePublicKeyForEncryption();
        flowKeyExchange(NetConfiguration.SECMEC_EUSRPWDDTA, publicKey_);


        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                null, //user
                null, //password
                encryptedUseridForEUSRIDPWD(),
                encryptedPasswordForEUSRIDPWD(password));
    }

    /**
     * The User ID and Strong Password Substitute mechanism (USRSSBPWD)
     * authenticates the user like the user ID and password mechanism, but
     * the password does not flow. A password substitute is generated instead
     * using the SHA-1 algorithm, and is sent to the application server.
     *
     * The application server generates a password substitute using the same
     * algorithm and compares it with the application requester's password
     * substitute. If equal, the user is authenticated.
     *
     * The SECTKN parameter is used to flow the client and server encryption
     * seeds on the ACCSEC and ACCSECRD commands.
     *
     * More information in DRDA, V3, Volume 3 standard - PWDSSB (page 650)
     */
    private void flowUSRSSBPWDconnect(String password) throws SqlException {
        flowServerAttributes();

        checkSecmgrForSecmecSupport(NetConfiguration.SECMEC_USRSSBPWD);
        // Generate a random client seed to send to the target server - in
        // response we will also get a generated seed from this last one.
        // Seeds are used on both sides to generate the password substitute.
        initializeClientSeed();

        flowSeedExchange(NetConfiguration.SECMEC_USRSSBPWD, sourceSeed_);

        flowSecurityCheckAndAccessRdb(targetSecmec_, //securityMechanism
                user_,
                null,
                null,
                passwordSubstituteForUSRSSBPWD(password)); // PWD Substitute
    }

    private void flowServerAttributes() throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        netAgent_.netConnectionRequest_.writeExchangeServerAttributes(extnam_, //externalName
                targetAgent_,
                netAgent_.targetSqlam_,
                targetRdb_,
                targetSecmgr_,
                targetCmntcpip_,
                targetCmnappc_,
                targetXamgr_,
                targetSyncptmgr_,
                targetRsyncmgr_,
                targetUnicodemgr_);
        agent_.flowOutsideUOW();
        netAgent_.netConnectionReply_.readExchangeServerAttributes(this);
        agent_.endReadChain();
    }

    private void flowKeyExchange(int securityMechanism, byte[] publicKey) throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        netAgent_.netConnectionRequest_.writeAccessSecurity(securityMechanism,
                databaseName_,
                publicKey);
        agent_.flowOutsideUOW();
        netAgent_.netConnectionReply_.readAccessSecurity(this, securityMechanism);
        agent_.endReadChain();
    }

    private void flowSeedExchange(int securityMechanism, byte[] sourceSeed) throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        netAgent_.netConnectionRequest_.writeAccessSecurity(securityMechanism,
                databaseName_,
                sourceSeed);
        agent_.flowOutsideUOW();
        netAgent_.netConnectionReply_.readAccessSecurity(this, securityMechanism);
        agent_.endReadChain();
    }

    private void flowServerAttributesAndKeyExchange(int securityMechanism,
                                                    byte[] publicKey) throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        writeServerAttributesAndKeyExchange(securityMechanism, publicKey);
        agent_.flowOutsideUOW();
        readServerAttributesAndKeyExchange(securityMechanism);
        agent_.endReadChain();
    }

    private void flowSecurityCheckAndAccessRdb(int securityMechanism,
                                               String user,
                                               String password,
                                               byte[] encryptedUserid,
                                               byte[] encryptedPassword) throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        writeSecurityCheckAndAccessRdb(securityMechanism,
                user,
                password,
                encryptedUserid,
                encryptedPassword);
        agent_.flowOutsideUOW();
        readSecurityCheckAndAccessRdb();
        agent_.endReadChain();
    }

    private void writeAllConnectCommandsChained(int securityMechanism,
                                                String user,
                                                String password) throws SqlException {
        writeServerAttributesAndKeyExchange(securityMechanism,
                null); // publicKey
        writeSecurityCheckAndAccessRdb(securityMechanism,
                user,
                password,
                null, //encryptedUserid
                null); //encryptedPassword,
    }

    private void readAllConnectCommandsChained(int securityMechanism) throws SqlException {
        readServerAttributesAndKeyExchange(securityMechanism);
        readSecurityCheckAndAccessRdb();
    }

    private void writeServerAttributesAndKeyExchange(int securityMechanism,
                                                     byte[] publicKey) throws SqlException {
        netAgent_.netConnectionRequest_.writeExchangeServerAttributes(extnam_, //externalName
                targetAgent_,
                netAgent_.targetSqlam_,
                targetRdb_,
                targetSecmgr_,
                targetCmntcpip_,
                targetCmnappc_,
                targetXamgr_,
                targetSyncptmgr_,
                targetRsyncmgr_,
                targetUnicodemgr_);
        netAgent_.netConnectionRequest_.writeAccessSecurity(securityMechanism,
                databaseName_,
                publicKey);
    }

    private void readServerAttributesAndKeyExchange(int securityMechanism) throws SqlException {
        netAgent_.netConnectionReply_.readExchangeServerAttributes(this);
        netAgent_.netConnectionReply_.readAccessSecurity(this, securityMechanism);
    }

    private void writeSecurityCheckAndAccessRdb(int securityMechanism,
                                                String user,
                                                String password,
                                                byte[] encryptedUserid,
                                                byte[] encryptedPassword) throws SqlException {
        netAgent_.netConnectionRequest_.writeSecurityCheck(securityMechanism,
                databaseName_,
                user,
                password,
                encryptedUserid,
                encryptedPassword);
        netAgent_.netConnectionRequest_.writeAccessDatabase(databaseName_,
                false,
                crrtkn_,
                prddta_.array(),
                netAgent_.typdef_);
    }

    private void readSecurityCheckAndAccessRdb() throws SqlException {
        netAgent_.netConnectionReply_.readSecurityCheck(this);
        netAgent_.netConnectionReply_.readAccessDatabase(this);
    }

    void writeDeferredReset() throws SqlException {
        // NetConfiguration.SECMEC_USRIDPWD
        if (securityMechanism_ == NetConfiguration.SECMEC_USRIDPWD) {
            writeAllConnectCommandsChained(NetConfiguration.SECMEC_USRIDPWD,
                    user_,
                    getDeferredResetPassword());
        }
        // NetConfiguration.SECMEC_USRIDONL
        else if (securityMechanism_ == NetConfiguration.SECMEC_USRIDONL) {
            writeAllConnectCommandsChained(NetConfiguration.SECMEC_USRIDONL,
                    user_,
                    null);  //password
        }
        // Either NetConfiguration.SECMEC_USRENCPWD,
        // NetConfiguration.SECMEC_EUSRIDPWD or
        // NetConfiguration.SECMEC_USRSSBPWD
        else {
            if (securityMechanism_ == NetConfiguration.SECMEC_USRSSBPWD)
                initializeClientSeed();
            else // SECMEC_USRENCPWD, SECMEC_EUSRIDPWD
                initializePublicKeyForEncryption();

            // Set the resetConnectionAtFirstSql_ to false to avoid going in an
            // infinite loop, since all the flow methods call beginWriteChain which then
            // calls writeDeferredResetConnection where the check for resetConnectionAtFirstSql_
            // is done. By setting the resetConnectionAtFirstSql_ to false will avoid calling the
            // writeDeferredReset method again.
            resetConnectionAtFirstSql_ = false;

            if (securityMechanism_ == NetConfiguration.SECMEC_USRSSBPWD)
                flowSeedExchange(securityMechanism_, sourceSeed_);
            else // SECMEC_USRENCPWD, SECMEC_EUSRIDPWD
                flowServerAttributesAndKeyExchange(securityMechanism_, publicKey_);

            agent_.beginWriteChainOutsideUOW();

            // Reset the resetConnectionAtFirstSql_ to true since we are done
            // with the flow method.
            resetConnectionAtFirstSql_ = true;

            // NetConfiguration.SECMEC_USRENCPWD
            if (securityMechanism_ == NetConfiguration.SECMEC_USRENCPWD) {
                writeSecurityCheckAndAccessRdb(NetConfiguration.SECMEC_USRENCPWD,
                        user_,
                        null, //password
                        null, //encryptedUserid
                        encryptedPasswordForUSRENCPWD(getDeferredResetPassword()));
            }
            // NetConfiguration.SECMEC_USRSSBPWD
            else if (securityMechanism_ == NetConfiguration.SECMEC_USRSSBPWD) {
                writeSecurityCheckAndAccessRdb(NetConfiguration.SECMEC_USRSSBPWD,
                        user_,
                        null,
                        null,
                        passwordSubstituteForUSRSSBPWD(getDeferredResetPassword()));
            }
            else {  // NetConfiguration.SECMEC_EUSRIDPWD
                writeSecurityCheckAndAccessRdb(NetConfiguration.SECMEC_EUSRIDPWD,
                        null, //user
                        null, //password
                        encryptedUseridForEUSRIDPWD(),
                        encryptedPasswordForEUSRIDPWD(getDeferredResetPassword()));
            }
        }
    }

    void readDeferredReset() throws SqlException {
        resetConnectionAtFirstSql_ = false;
        // either NetConfiguration.SECMEC_USRIDPWD or NetConfiguration.SECMEC_USRIDONL
        if (securityMechanism_ == NetConfiguration.SECMEC_USRIDPWD ||
                securityMechanism_ == NetConfiguration.SECMEC_USRIDONL) {
            readAllConnectCommandsChained(securityMechanism_);
        }
        // either NetConfiguration.SECMEC_USRENCPWD or NetConfiguration.SECMEC_EUSRIDPWD
        else {
            // either NetConfiguration.SECMEC_USRENCPWD or NetConfiguration.SECMEC_EUSRIDPWD
            readSecurityCheckAndAccessRdb();
        }
        if (agent_.loggingEnabled()) {
            agent_.logWriter_.traceConnectResetExit(this);
        }
    }

    //-------------------parse callback methods--------------------------------

    void setServerAttributeData(String srvclsnm,
                                String srvrlslv) {
        targetSrvclsnm_ = srvclsnm;      // since then can be optionally returned from the
                                         // server
        targetSrvrlslv_ = srvrlslv;
    }

    // secmecList is always required and will not be null.
    // secchkcd has an implied severity of error.
    // it will be returned if an error is detected.
    // if no errors and security mechanism requires a sectkn, then
    void setAccessSecurityData(int secchkcd,
                               int desiredSecmec,
                               int[] secmecList,
                               boolean sectknReceived,
                               byte[] sectkn) throws DisconnectException {
        // - if the secchkcd is not 0, then map to an exception.
        if (secchkcd != CodePoint.SECCHKCD_00) {
            // the implied severity code is error
            netAgent_.setSvrcod(CodePoint.SVRCOD_ERROR);
            agent_.accumulateReadException(mapSecchkcd(secchkcd));
        } else {
            // - verify that the secmec parameter reflects the value sent
            // in the ACCSEC command.
            // should we check for null list
            if ((secmecList.length == 1) &&
                    (secmecList[0] == desiredSecmec)) {
                // the security mechanism returned from the server matches
                // the mechanism requested by the client.
                targetSecmec_ = secmecList[0];

                if ((targetSecmec_ == NetConfiguration.SECMEC_USRENCPWD) ||
                        (targetSecmec_ == NetConfiguration.SECMEC_EUSRIDPWD) ||
                        (targetSecmec_ == NetConfiguration.SECMEC_USRSSBPWD) ||
                        (targetSecmec_ == NetConfiguration.SECMEC_EUSRIDDTA) ||
                        (targetSecmec_ == NetConfiguration.SECMEC_EUSRPWDDTA)) {

                    // a security token is required for USRENCPWD, or EUSRIDPWD.
                    if (!sectknReceived) {
                        agent_.accumulateChainBreakingReadExceptionAndThrow(
                            new DisconnectException(agent_, 
                                new ClientMessageId(SQLState.NET_SECTKN_NOT_RETURNED)));
                    } else {
                        if (targetSecmec_ == NetConfiguration.SECMEC_USRSSBPWD)
                            targetSeed_ = sectkn;
                        else
                            targetPublicKey_ = sectkn;
                        if (encryptionManager_ != null) {
                            encryptionManager_.resetSecurityKeys();
                        }
                    }
                }
            } else {
                // accumulate an SqlException and don't disconnect yet
                // if a SECCHK was chained after this it would receive a secchk code
                // indicating the security mechanism wasn't supported and that would be a
                // chain breaking exception.  if no SECCHK is chained this exception
                // will be surfaced by endReadChain
                // agent_.accumulateChainBreakingReadExceptionAndThrow (
                //   new DisconnectException (agent_,"secmec not supported ","0000", -999));
                agent_.accumulateReadException(new SqlException(agent_.logWriter_, 
                    new ClientMessageId(SQLState.NET_SECKTKN_NOT_RETURNED)));
            }
        }
    }

    void securityCheckComplete(int svrcod, int secchkcd) {
        netAgent_.setSvrcod(svrcod);
        if (secchkcd == CodePoint.SECCHKCD_00) {
            return;
        }
        agent_.accumulateReadException(mapSecchkcd(secchkcd));
    }

    void rdbAccessed(int svrcod,
                     String prdid,
                     boolean crrtknReceived,
                     byte[] crrtkn) {
        if (crrtknReceived) {
            crrtkn_ = crrtkn;
        }

        netAgent_.setSvrcod(svrcod);
        productID_ = prdid;
    }


    //-------------------Abstract object factories--------------------------------

    protected Agent newAgent_(
            LogWriter logWriter,
            int loginTimeout,
            String serverName,
            int portNumber,
            int clientSSLMode) throws SqlException {

        return new NetAgent(this,
                logWriter,
                loginTimeout,
                serverName,
                portNumber,
                clientSSLMode);
    }


    protected ClientStatement newStatement_(
            int type,
            int concurrency,
            int holdability) throws SqlException {

        return new NetStatement(netAgent_, this, type, concurrency, holdability).statement_;
    }

    protected void resetStatement_(
            ClientStatement statement,
            int type,
            int concurrency,
            int holdability) throws SqlException {

        ((NetStatement) statement.getMaterialStatement()).resetNetStatement(netAgent_, this, type, concurrency, holdability);
    }

    protected ClientPreparedStatement newPositionedUpdatePreparedStatement_(
            String sql,
            Section section) throws SqlException {

        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetPreparedStatement(netAgent_, this, sql, section,pooledConnection_).preparedStatement_;
    }

    protected ClientPreparedStatement newPreparedStatement_(
            String sql,
            int type,
            int concurrency,
            int holdability,
            int autoGeneratedKeys,
            String[] columnNames,
            int[] columnIndexes) throws SqlException {
        
        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetPreparedStatement(netAgent_, this, sql, type, concurrency, holdability, autoGeneratedKeys, columnNames,
                columnIndexes, pooledConnection_).preparedStatement_;
    }

    protected void resetPreparedStatement_(ClientPreparedStatement ps,
                                           String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability,
                                           int autoGeneratedKeys,
                                           String[] columnNames,
                                           int[] columnIndexes) throws SqlException {
        ((NetPreparedStatement) ps.materialPreparedStatement_).resetNetPreparedStatement(netAgent_, this, sql, resultSetType, resultSetConcurrency, 
                resultSetHoldability, autoGeneratedKeys, columnNames, columnIndexes);
    }


    protected ClientCallableStatement newCallableStatement_(
            String sql,
            int type,
            int concurrency,
            int holdability) throws SqlException {

        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetCallableStatement(netAgent_, this, sql, type, concurrency, holdability,pooledConnection_).callableStatement_;
    }

    protected void resetCallableStatement_(ClientCallableStatement cs,
                                           String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) throws SqlException {
        ((NetCallableStatement) cs.materialCallableStatement_).resetNetCallableStatement(netAgent_, this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }


    protected ClientDatabaseMetaData newDatabaseMetaData_() {
            return ClientDriver.getFactory().newNetDatabaseMetaData(netAgent_, this);
    }

    //-------------------private helper methods--------------------------------

    private void checkDatabaseName() throws SqlException {
        // netAgent_.logWriter may not be initialized yet
        if (databaseName_ == null) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.CONNECT_REQUIRED_PROPERTY_NOT_SET),
                "databaseName");
        }
    }

    private void checkUserLength(String user) throws SqlException {
        int usridLength = user.length();
        if ((usridLength == 0) || (usridLength > NetConfiguration.USRID_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.CONNECT_USERID_LENGTH_OUT_OF_RANGE),
                usridLength, NetConfiguration.USRID_MAXSIZE);
        }
    }

    private void checkPasswordLength(String password) throws SqlException {
        int passwordLength = password.length();
        if ((passwordLength == 0) || (passwordLength > NetConfiguration.PASSWORD_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.CONNECT_PASSWORD_LENGTH_OUT_OF_RANGE),
                passwordLength, NetConfiguration.PASSWORD_MAXSIZE);
        }
    }

    private void checkUser(String user) throws SqlException {
        if (user == null) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.CONNECT_USERID_ISNULL));
        }
        checkUserLength(user);
    }

    private void checkUserPassword(String user, String password) throws SqlException {
        checkUser(user);
        if (password == null) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.CONNECT_PASSWORD_ISNULL));
        }
        checkPasswordLength(password);
    }


    // Determine if a security mechanism is supported by
    // the security manager used for the connection.
    // An exception is thrown if the security mechanism is not supported
    // by the secmgr.
    private void checkSecmgrForSecmecSupport(int securityMechanism) throws SqlException {
        boolean secmecSupported = false;

        // Point to a list (array) of supported security mechanisms.
        int[] supportedSecmecs = NetConfiguration.SECMGR_SECMECS;

        // check to see if the security mechanism is on the supported list.
        for (int i = 0; (i < supportedSecmecs.length) && (!secmecSupported); i++) {
            if (supportedSecmecs[i] == securityMechanism) {
                secmecSupported = true;
            }
        }

        // throw an exception if not supported (not on list).
        if (!secmecSupported) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.SECMECH_NOT_SUPPORTED),
                securityMechanism);
        }
    }

    // If secchkcd is not 0, map to SqlException
    // according to the secchkcd received.
    private SqlException mapSecchkcd(int secchkcd) {
        if (secchkcd == CodePoint.SECCHKCD_00) {
            return null;
        }

        // the net driver will not support new password at this time.
        // Here is the message for -30082 (STATE "08001"):
        //    Attempt to establish connection failed with security
        //    reason {0} {1} +  reason-code + reason-string.
        switch (secchkcd) {
        case CodePoint.SECCHKCD_01:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_SECMECH_NOT_SUPPORTED));
        case CodePoint.SECCHKCD_10:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_PASSWORD_MISSING));
        case CodePoint.SECCHKCD_12:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_USERID_MISSING));
        case CodePoint.SECCHKCD_13:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_USERID_OR_PASSWORD_INVALID));
        case CodePoint.SECCHKCD_14:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_USERID_REVOKED));
        case CodePoint.SECCHKCD_15:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_NEW_PASSWORD_INVALID));
        case CodePoint.SECCHKCD_0A:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_SECSVC_NONRETRYABLE_ERR));
        case CodePoint.SECCHKCD_0B:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_SECTKN_MISSING_OR_INVALID));
        case CodePoint.SECCHKCD_0E:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_PASSWORD_EXPIRED));
        case CodePoint.SECCHKCD_0F:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_USERID_OR_PASSWORD_INVALID));
        default:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NET_CONNECT_AUTH_FAILED),
                msgutil.getTextMessage(MessageId.CONN_NOT_SPECIFIED));
        }
    }

    // Construct the correlation token.
    // The crrtkn has the following format.
    //
    // <Almost IP address>.<local port number><current time in millis>
    // |                   | |               ||                  |
    // +----+--------------+ +-----+---------++---------+--------+
    //      |                      |                |
    //    8 bytes               4 bytes         6 bytes
    // Total lengtho of 19 bytes.
    //
    // 1 char for each 1/2 byte in the IP address.
    // If the first character of the <IP address> or <port number>
    // starts with '0' thru '9', it will be mapped to 'G' thru 'P'.
    // Reason for mapping the IP address is in order to use the crrtkn as the LUWID when using SNA in a hop site.
    protected void constructCrrtkn() throws SqlException {
        // allocate the crrtkn array.
        if (crrtkn_ == null) {
            crrtkn_ = new byte[19];
        } else {
            Arrays.fill(crrtkn_, (byte) 0);
        }

        byte [] localAddressBytes = netAgent_.socket_.getLocalAddress().getAddress();

        // IP addresses are returned in a 4 byte array.
        // Obtain the character representation of each half byte.
        for (int i = 0, j = 0; i < 4; i++, j += 2) {

            // since a byte is signed in java, convert any negative
            // numbers to positive before shifting.
            int num = localAddressBytes[i] < 0 ? localAddressBytes[i] + 256 : localAddressBytes[i];
            int halfByte = (num >> 4) & 0x0f;

            // map 0 to G
            // The first digit of the IP address is is replaced by
            // the characters 'G' thro 'P'(in order to use the crrtkn as the LUWID when using
            // SNA in a hop site). For example, 0 is mapped to G, 1 is mapped H,etc.
            if (i == 0) {
                crrtkn_[j] = netAgent_.getCurrentCcsidManager().numToSnaRequiredCrrtknChar_[halfByte];
            } else {
                crrtkn_[j] = netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
            }

            halfByte = (num) & 0x0f;
            crrtkn_[j + 1] = netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        }

        // fill the '.' in between the IP address and the port number
        crrtkn_[8] = netAgent_.getCurrentCcsidManager().dot_;

        // Port numbers have values which fit in 2 unsigned bytes.
        // Java returns port numbers in an int so the value is not negative.
        // Get the character representation by converting the
        // 4 low order half bytes to the character representation.
        int num = netAgent_.socket_.getLocalPort();

        int halfByte = (num >> 12) & 0x0f;
        crrtkn_[9] = netAgent_.getCurrentCcsidManager().numToSnaRequiredCrrtknChar_[halfByte];
        halfByte = (num >> 8) & 0x0f;
        crrtkn_[10] = netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        halfByte = (num >> 4) & 0x0f;
        crrtkn_[11] = netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];
        halfByte = (num) & 0x0f;
        crrtkn_[12] = netAgent_.getCurrentCcsidManager().numToCharRepresentation_[halfByte];

        // The final part of CRRTKN is a 6 byte binary number that makes the
        // crrtkn unique, which is usually the time stamp/process id.
        // If the new time stamp is the
        // same as one of the already created ones, then recreate the time stamp.
        long time = System.currentTimeMillis();

        for (int i = 0; i < 6; i++) {
            // store 6 bytes of 8 byte time into crrtkn
            crrtkn_[i + 13] = (byte) (time >>> (40 - (i * 8)));
        }
    }


    private void constructExtnam() throws SqlException {
        /* Construct the EXTNAM based on the thread name */
        char[] chars = Thread.currentThread().getName().toCharArray();

        /* DERBY-4584: Replace non-EBCDIC characters (> 0xff) with '?' */
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] > 0xff) chars[i] = '?';
        }
        extnam_ = "derbydnc" + new String(chars);
    }

    private void constructPrddta() throws SqlException {
        if (prddta_ == null) {
            prddta_ = ByteBuffer.allocate(NetConfiguration.PRDDTA_MAXSIZE);
        } else {
            prddta_.clear();
            Arrays.fill(prddta_.array(), (byte) 0);
        }

        CcsidManager ccsidMgr = netAgent_.getCurrentCcsidManager();

        for (int i = 0; i < NetConfiguration.PRDDTA_ACCT_SUFFIX_LEN_BYTE; i++) {
            prddta_.put(i, ccsidMgr.space_);
        }

        // Start inserting data right after the length byte.
        prddta_.position(NetConfiguration.PRDDTA_LEN_BYTE + 1);

        // Register the success of the encode operations for verification in
        // sane mode.
        boolean success = true;

        ccsidMgr.startEncoding();
        success &= ccsidMgr.encode(
                CharBuffer.wrap(NetConfiguration.PRDID), prddta_, agent_);

        ccsidMgr.startEncoding();
        success &= ccsidMgr.encode(
                CharBuffer.wrap(NetConfiguration.PRDDTA_PLATFORM_ID),
                prddta_, agent_);

        int prddtaLen = prddta_.position();

        int extnamTruncateLength = Math.min(extnam_.length(), NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN);
        ccsidMgr.startEncoding();
        success &= ccsidMgr.encode(
                CharBuffer.wrap(extnam_, 0, extnamTruncateLength),
                prddta_, agent_);

        if (SanityManager.DEBUG) {
            // The encode() calls above should all complete without overflow,
            // since we control the contents of the strings. Verify this in
            // sane mode so that we notice it if the strings change so that
            // they go beyond the max size of PRDDTA.
            SanityManager.ASSERT(success,
                "PRDID, PRDDTA_PLATFORM_ID and EXTNAM exceeded PRDDTA_MAXSIZE");
        }

        prddtaLen += NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN;

        prddtaLen += NetConfiguration.PRDDTA_USER_ID_FIXED_LEN;

        // Mark that we have an empty suffix in PRDDTA_ACCT_SUFFIX_LEN_BYTE.
        prddta_.put(NetConfiguration.PRDDTA_ACCT_SUFFIX_LEN_BYTE, (byte) 0);
        prddtaLen++;

        // the length byte value does not include itself.
        prddta_.put(NetConfiguration.PRDDTA_LEN_BYTE, (byte) (prddtaLen - 1));
    }

    private void initializePublicKeyForEncryption() throws SqlException {
        if (encryptionManager_ == null) {
            encryptionManager_ = new EncryptionManager(agent_);
        }
        publicKey_ = encryptionManager_.obtainPublicKey();
    }

    // SECMEC_USRSSBPWD security mechanism - Generate a source (client) seed
    // to send to the target (application) server.
    private void initializeClientSeed() throws SqlException {
        if (encryptionManager_ == null) {
            encryptionManager_ = new EncryptionManager(
                                    agent_,
                                    EncryptionManager.SHA_1_DIGEST_ALGORITHM);
        }
        sourceSeed_ = encryptionManager_.generateSeed();
    }

    private byte[] encryptedPasswordForUSRENCPWD(String password) throws SqlException {
        return encryptionManager_.encryptData(netAgent_.getCurrentCcsidManager().convertFromJavaString(password, netAgent_),
                NetConfiguration.SECMEC_USRENCPWD,
                netAgent_.getCurrentCcsidManager().convertFromJavaString(user_, netAgent_),
                targetPublicKey_);
    }

    private byte[] encryptedUseridForEUSRIDPWD() throws SqlException {
        return encryptionManager_.encryptData(netAgent_.getCurrentCcsidManager().convertFromJavaString(user_, netAgent_),
                NetConfiguration.SECMEC_EUSRIDPWD,
                targetPublicKey_,
                targetPublicKey_);
    }

    private byte[] encryptedPasswordForEUSRIDPWD(String password) throws SqlException {
        return encryptionManager_.encryptData(netAgent_.getCurrentCcsidManager().convertFromJavaString(password, netAgent_),
                NetConfiguration.SECMEC_EUSRIDPWD,
                targetPublicKey_,
                targetPublicKey_);
    }

    private byte[] passwordSubstituteForUSRSSBPWD(String password) throws SqlException {
        String userName = user_;
        
        // Define which userName takes precedence - If we have a dataSource
        // available here, it is posible that the userName has been
        // overriden by some defined as part of the connection attributes
        // (see BasicClientDataSource40.updateDataSourceValues().
        // We need to use the right userName as strong password
        // substitution depends on the userName when the substitute
        // password is generated; if we were not using the right userName
        // then authentication would fail when regenerating the substitute
        // password on the engine server side, where userName as part of the
        // connection attributes would get used to authenticate the user.
        if (dataSource_ != null)
        {
            String dataSourceUserName = dataSource_.getUser();
            if (!dataSourceUserName.equals("") &&
                userName.equalsIgnoreCase(
                    ClientDataSourceInterface.propertyDefault_user) &&
                !dataSourceUserName.equalsIgnoreCase(
                    ClientDataSourceInterface.propertyDefault_user))
            {
                userName = dataSourceUserName;
            }
        }
        return encryptionManager_.substitutePassword(
                userName, password, sourceSeed_, targetSeed_);
    }

    // Methods to get the manager levels for Regression harness only.
    public int getSQLAM() {
        return netAgent_.targetSqlam_;
    }

    public int getAGENT() {
        return targetAgent_;
    }

    public int getCMNTCPIP() {
        return targetCmntcpip_;
    }

    public int getRDB() {
        return targetRdb_;
    }

    public int getSECMGR() {
        return targetSecmgr_;
    }

    public int getXAMGR() {
        return targetXamgr_;
    }

    public int getSYNCPTMGR() {
        return targetSyncptmgr_;
    }

    public int getRSYNCMGR() {
        return targetRsyncmgr_;
    }


    private char[] flipBits(char[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] ^= 0xff;
        }
        return array;
    }

    public void writeCommitSubstitute_() throws SqlException {
        netAgent_.connectionRequest_.writeCommitSubstitute(this);
    }

    public void readCommitSubstitute_() throws SqlException {
        netAgent_.connectionReply_.readCommitSubstitute(this);
    }

    public void writeLocalXAStart_() throws SqlException {
        netAgent_.connectionRequest_.writeLocalXAStart(this);
    }

    public void readLocalXAStart_() throws SqlException {
        netAgent_.connectionReply_.readLocalXAStart(this);
    }

    public void writeLocalXACommit_() throws SqlException {
        netAgent_.connectionRequest_.writeLocalXACommit(this);
    }

    public void readLocalXACommit_() throws SqlException {
        netAgent_.connectionReply_.readLocalXACommit(this);
    }

    public void writeLocalXARollback_() throws SqlException {
        netAgent_.connectionRequest_.writeLocalXARollback(this);
    }

    public void readLocalXARollback_() throws SqlException {
        netAgent_.connectionReply_.readLocalXARollback(this);
    }

    public void writeLocalCommit_() throws SqlException {
        netAgent_.connectionRequest_.writeLocalCommit(this);
    }

    public void readLocalCommit_() throws SqlException {
        netAgent_.connectionReply_.readLocalCommit(this);
    }

    public void writeLocalRollback_() throws SqlException {
        netAgent_.connectionRequest_.writeLocalRollback(this);
    }

    public void readLocalRollback_() throws SqlException {
        netAgent_.connectionReply_.readLocalRollback(this);
    }


    protected void markClosed_() {
    }

    protected boolean isGlobalPending_() {
        return false;
    }

    protected boolean doCloseStatementsOnClose_() {
        return closeStatementsOnClose;
    }

    /**
     * Check if the connection can be closed when there are uncommitted
     * operations.
     *
     * @return if this connection can be closed when it has uncommitted
     * operations, {@code true}; otherwise, {@code false}
     */
    protected boolean allowCloseInUOW_() {
        // We allow closing in unit of work in two cases:
        //
        //   1) if auto-commit is on, since then Connection.close() will cause
        //   a commit so we won't leave uncommitted changes around
        //
        //   2) if we're not allowed to commit or roll back the transaction via
        //   the connection object (because the it is part of an XA
        //   transaction). In that case, commit and rollback are performed via
        //   the XAResource, and it is therefore safe to close the connection.
        //
        // Otherwise, the transaction must be idle before a call to close() is
        // allowed.

        return autoCommit_ || !allowLocalCommitRollback_();
    }

    // Driver-specific determination if local COMMIT/ROLLBACK is allowed;
    // Allow local COMMIT/ROLLBACK only if we are not in an XA transaction
    protected boolean allowLocalCommitRollback_() {
       
        if (getXAState() == XA_T0_NOT_ASSOCIATED) {
            return true;
        }
        return false;
    }

    public void setInputStream(InputStream inputStream) {
        netAgent_.setInputStream(inputStream);
    }

    public void setOutputStream(OutputStream outputStream) {
        netAgent_.setOutputStream(outputStream);
    }

    public InputStream getInputStream() {
        return netAgent_.getInputStream();
    }

    public OutputStream getOutputStream() {
        return netAgent_.getOutputStream();
    }


    public void writeTransactionStart(ClientStatement statement)
            throws SqlException {
    }

    public void readTransactionStart() throws SqlException {
        super.readTransactionStart();
    }

    public void setIndoubtTransactions(
            List<Xid> indoubtTransactions) {

        if (isXAConnection_) {
            if (indoubtTransactions_ != null) {
                indoubtTransactions_.clear();
            }
            indoubtTransactions_ = indoubtTransactions;
        }
    }

    Xid[] getIndoubtTransactionIds() {
        Xid[] result = new Xid[0];
        return indoubtTransactions_.toArray(result);
    }

    public SectionManager newSectionManager
            (Agent agent) {
        return new SectionManager(agent);
    }

    public boolean willAutoCommitGenerateFlow() {
        // this logic must be in sync with writeCommit() logic
        if (!autoCommit_) {
            return false;
        }
        if (!isXAConnection_) {
            return true;
        }
        boolean doCommit = false;
        int xaState = getXAState();

        
        if (xaState == XA_T0_NOT_ASSOCIATED) {
            doCommit = true;
        }

        return doCommit;
    }

    public int getSecurityMechanism() {
        return securityMechanism_;
    }

    public EncryptionManager getEncryptionManager() {
        return encryptionManager_;
    }

    public byte[] getTargetPublicKey() {
        return targetPublicKey_ != null ?
               targetPublicKey_.clone() :
               null;
    }

    public String getProductID() {
        return targetSrvclsnm_;
    }

    /**
     * @return Returns the connectionNull.
     */
    public final boolean isConnectionNull() {
        return connectionNull;
    }
    /**
     * @param connectionNull The connectionNull to set.
     */
    public void setConnectionNull(boolean connectionNull) {
        this.connectionNull = connectionNull;
    }

    /**
     * Check whether the server has full support for the QRYCLSIMP
     * parameter in OPNQRY.
     *
     * @return true if QRYCLSIMP is fully supported
     */
    final boolean serverSupportsQryclsimp() {
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;
        return metadata.serverSupportsQryclsimp();
    }

    
    public final boolean serverSupportsLayerBStreaming() {
        
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;
        
        return metadata.serverSupportsLayerBStreaming();

    }
    
    public final boolean serverSupportLongRDBNAM() {
        
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;
        
        return metadata.serverSupportLongRDBNAM();

    }
    
    
    /**
     * Check whether the server supports session data caching
     * @return true session data caching is supported
     */
    protected final boolean supportsSessionDataCaching() {

        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;

        return metadata.serverSupportsSessionDataCaching();
    }

    /**
     * Check whether the server supports the UTF-8 Ccsid Manager
     * @return true if the server supports the UTF-8 Ccsid Manager
     */
    protected final boolean serverSupportsUtf8Ccsid() {
        return targetUnicodemgr_ == CcsidManager.UTF8_CCSID;
    }
    
    /**
     * Check whether the server supports UDTs
     * @return true if UDTs are supported
     */
    protected final boolean serverSupportsUDTs() {

        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;

        return metadata.serverSupportsUDTs();
    }

    protected final boolean serverSupportsEXTDTAAbort() {
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;

        return metadata.serverSupportsEXTDTAAbort();
    }

    /**
     * Checks whether the server supports locators for large objects.
     *
     * @return {@code true} if LOB locators are supported.
     */
    protected final boolean serverSupportsLocators() {
        // Support for locators was added in the same version as layer B
        // streaming.
        return serverSupportsLayerBStreaming();
    }

    /** Return true if the server supports nanoseconds in timestamps */
    protected final boolean serverSupportsTimestampNanoseconds()
    {
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;

        return metadata.serverSupportsTimestampNanoseconds();
    }
    
    /**
     * Returns if a transaction is in process
     * @return open
     */
    public boolean isOpen() {
        return open_;
    }
    
    /**
     * Invokes write commit on NetXAConnection
     */
    protected void writeXACommit_() throws SqlException {
        xares_.netXAConn_.writeCommit();
    }
    
    /**
     * Invokes readCommit on NetXAConnection
     */
    protected void readXACommit_() throws SqlException {
        xares_.netXAConn_.readCommit();
    }
    
    /**
     * Invokes writeRollback on NetXAConnection
     */
    protected void writeXARollback_() throws SqlException {
        xares_.netXAConn_.writeRollback();
    }
    
    /**
     * Invokes writeRollback on NetXAConnection
     */
    protected void readXARollback_() throws SqlException {
            xares_.netXAConn_.readRollback();
    }
    
    
    protected void writeXATransactionStart(ClientStatement statement)
            throws SqlException {
        xares_.netXAConn_.writeTransactionStart(statement);
    }

    // JDBC 4.0 methods

    public Array createArrayOf(String typeName, Object[] elements)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createArrayOf(String,Object[])");
    }

    public NClob createNClob() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createNClob ()");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createSQLXML ()");
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createStruct(String,Object[])");
    }

    /**
     * Checks if the connection has not been closed and is still valid.
     * The validity is checked by running a simple query against the
     * database.
     *
     * The timeout specified by the caller is implemented as follows:
     * On the server: uses the queryTimeout functionality to make the
     * query time out on the server in case the server has problems or
     * is highly loaded.
     * On the client: uses a timeout on the socket to make sure that
     * the client is not blocked forever in the cases where the server
     * is "hanging" or not sending the reply.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the
     * timeout period expires before the operation completes, this
     * method returns false. A value of 0 indicates a timeout is not
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the parameter value is illegal or if a
     * database error has occurred
     */
    public boolean isValid(int timeout) throws SQLException {
        // Validate that the timeout has a legal value
        if (timeout < 0) {
            throw new SqlException(agent_.logWriter_,
                               new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                               timeout, "timeout",
                               "java.sql.Connection.isValid" ).getSQLException();
        }

        // Check if the connection is closed
        if (isClosed()) {
            return false;
        }

        // Do a simple query against the database
        synchronized(this) {
            try {
                // Save the current network timeout value
                int oldTimeout = netAgent_.getTimeout();

                // Set the required timeout value on the network connection
                netAgent_.setTimeout(timeout);

                // If this is the first time this method is called on this
                // connection we prepare the query
                if (isValidStmt == null) {
                    isValidStmt = prepareStatement("VALUES (1)");
                }

                // Set the query timeout
                isValidStmt.setQueryTimeout(timeout);

                // Run the query against the database
                ResultSet rs = isValidStmt.executeQuery();
                rs.close();

                // Restore the previous timeout value
                netAgent_.setTimeout(oldTimeout);
            } catch(SQLException e) {
                // If an SQL exception is thrown the connection is not valid,
                // we ignore the exception and return false.
                return false;
            }
     }

        return true;  // The connection is valid
    }

    /**
     * Close the connection and release its resources.
     * @exception SQLException if a database-access error occurs.
     */
    synchronized public void close() throws SQLException {
        // Release resources owned by the prepared statement used by isValid
        if (isValidStmt != null) {
            isValidStmt.close();
            isValidStmt = null;
        }
        super.close();
    }

    /**
     * <code>setClientInfo</code> will always throw a
     * <code>SQLClientInfoException</code> since Derby does not support
     * any properties.
     *
     * @param name a property key <code>String</code>
     * @param value a property value <code>String</code>
     * @exception SQLClientInfoException always.
     */
    public void setClientInfo(String name, String value)
    throws SQLClientInfoException{
        Properties p = FailedProperties40.makeProperties(name,value);
    try { checkForClosedConnection(); }
    catch (SqlException se) {
            throw new SQLClientInfoException
                (se.getMessage(), se.getSQLState(),
                        se.getErrorCode(),
                        new FailedProperties40(p).getProperties());
        }

        if (name == null && value == null) {
            return;
        }
        setClientInfo(p);
    }

    /**
     * <code>setClientInfo</code> will throw a
     * <code>SQLClientInfoException</code> unless the <code>properties</code>
     * parameter is empty, since Derby does not support any
     * properties. All the property keys in the
     * <code>properties</code> parameter are added to failedProperties
     * of the exception thrown, with REASON_UNKNOWN_PROPERTY as the
     * value.
     *
     * @param properties a <code>Properties</code> object with the
     * properties to set.
     * @exception SQLClientInfoException unless the properties
     * parameter is null or empty.
     */
    public void setClientInfo(Properties properties)
    throws SQLClientInfoException {
    FailedProperties40 fp = new FailedProperties40(properties);
    try { checkForClosedConnection(); }
    catch (SqlException se) {
        throw new SQLClientInfoException(se.getMessage(), se.getSQLState(),
                se.getErrorCode(),
                fp.getProperties());
    }

    if (properties == null || properties.isEmpty()) {
            return;
        }

    SqlException se =
        new SqlException(agent_.logWriter_,
                 new ClientMessageId
                 (SQLState.PROPERTY_UNSUPPORTED_CHANGE),
                 fp.getFirstKey(), fp.getFirstValue());
        throw new SQLClientInfoException(se.getMessage(),
                se.getSQLState(),
                se.getErrorCode(),
                fp.getProperties());
    }

    /**
     * <code>getClientInfo</code> always returns a
     * <code>null String</code> since Derby doesn't support
     * ClientInfoProperties.
     *
     * @param name a <code>String</code> value
     * @return a <code>null String</code> value
     * @exception SQLException if the connection is closed.
     */
    public String getClientInfo(String name)
    throws SQLException{
    try {
        checkForClosedConnection();
        return null;
    }
    catch (SqlException se) { throw se.getSQLException(); }
    }

    /**
     * <code>getClientInfo</code> always returns an empty
     * <code>Properties</code> object since Derby doesn't support
     * ClientInfoProperties.
     *
     * @return an empty <code>Properties</code> object.
     * @exception SQLException if the connection is closed.
     */
    public Properties getClientInfo()
    throws SQLException{
    try {
        checkForClosedConnection();
        return new Properties();
    }
    catch (SqlException se) { throw se.getSQLException(); }
    }

    /**
     * Returns false unless <code>interfaces</code> is implemented
     *
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or
     *                                directly or indirectly wraps an object
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining
     *                                whether this is a wrapper for an object
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        return interfaces.isInstance(this);
    }

    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLException if no object if found that implements the
     * interface
     */
    public <T> T unwrap(Class<T> interfaces)
                                   throws SQLException {
        try {
            checkForClosedConnection();
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                interfaces).getSQLException();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }
}
