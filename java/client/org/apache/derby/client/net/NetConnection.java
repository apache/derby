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

import java.sql.SQLException;
import org.apache.derby.client.am.CallableStatement;
import org.apache.derby.client.am.DatabaseMetaData;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.EncryptionManager;
import org.apache.derby.client.am.PreparedStatement;
import org.apache.derby.client.am.ProductLevel;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.client.am.Statement;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.client.ClientPooledConnection;

import org.apache.derby.shared.common.reference.SQLState;

public class NetConnection extends org.apache.derby.client.am.Connection {
    
    // Use this to get internationalized strings...
    protected static MessageUtil msgutil = SqlException.getMessageUtil();

    protected NetAgent netAgent_;
    //contains a reference to the PooledConnection from which this created 
    //It then passes this reference to the PreparedStatement created from it
    //The PreparedStatement then uses this to pass the close and the error
    //occurred conditions back to the PooledConnection which can then throw the 
    //appropriate events.
    private final ClientPooledConnection pooledConnection_;
    private final boolean closeStatementsOnClose;

    // For XA Transaction
    protected int pendingEndXACallinfoOffset_ = -1;

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
    public int targetSecmgr_ = NetConfiguration.MGRLVL_7;
    protected int targetCmnappc_ = NetConfiguration.MGRLVL_NA;  //NA since currently not used by net
    protected int targetXamgr_ = NetConfiguration.MGRLVL_7;
    protected int targetSyncptmgr_ = NetConfiguration.MGRLVL_NA;
    protected int targetRsyncmgr_ = NetConfiguration.MGRLVL_NA;


    // this is the external name of the target server.
    // it is set by the parseExcsatrd method but not really used for much at this
    // time.  one possible use is for logging purposes and in the future it
    // may be placed in the trace.
    String targetExtnam_;
    String extnam_;

    // Server Class Name of the target server returned in excsatrd.
    // Again this is something which the driver is not currently using
    // to make any decions.  Right now it is just stored for future logging.
    // It does contain some useful information however and possibly
    // the database meta data object will make use of this
    // for example, the product id (prdid) would give this driver an idea of
    // what type of sevrer it is connected to.
    public String targetSrvclsnm_;

    // Server Name of the target server returned in excsatrd.
    // Again this is something which we don't currently use but
    // keep it in case we want to log it in some problem determination
    // trace/dump later.
    protected String targetSrvnam_;

    // Server Product Release Level of the target server returned in excsatrd.
    // specifies the procuct release level of a ddm server.
    // Again this is something which we don't currently use but
    // keep it in case we want to log it in some problem determination
    // trace/dump later.
    public String targetSrvrlslv_;

    // Keys used for encryption.
    transient byte[] publicKey_;
    transient byte[] targetPublicKey_;

    // Seeds used for strong password substitute generation (USRSSBPWD)
    transient byte[] sourceSeed_;   // Client seed
    transient byte[] targetSeed_;   // Server seed

    // Product-Specific Data (prddta) sent to the server in the accrdb command.
    // The prddta has a specified format.  It is saved in case it is needed again
    // since it takes a little effort to compute.  Saving this information is
    // useful for when the connect flows need to be resent (right now the connect
    // flow is resent when this driver disconnects and reconnects with
    // non unicode ccsids.  this is done when the server doesn't recoginze the
    // unicode ccsids).
    //

    byte[] prddta_;

    // Correlation Token of the source sent to the server in the accrdb.
    // It is saved like the prddta in case it is needed for a connect reflow.
    public byte[] crrtkn_;

    // The Secmec used by the target.
    // It contains the negotiated security mechanism for the connection.
    // Initially the value of this is 0.  It is set only when the server and
    // the target successfully negotiate a security mechanism.
    int targetSecmec_;

    // the security mechanism requested by the application
    protected int securityMechanism_;

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

    protected byte[] cnntkn_ = null;

    // resource manager Id for XA Connections.
    private int rmId_ = 0;
    protected NetXAResource xares_ = null;
    protected java.util.Hashtable indoubtTransactions_ = null;
    protected int currXACallInfoOffset_ = 0;
    private short seqNo_ = 1;

    // Flag to indicate a read only transaction
    protected boolean readOnlyTransaction_ = true;

    //---------------------constructors/finalizer---------------------------------

    public NetConnection(NetLogWriter netLogWriter,
                         String databaseName,
                         java.util.Properties properties) throws SqlException {
        super(netLogWriter, 0, "", -1, databaseName, properties);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
    }

    public NetConnection(NetLogWriter netLogWriter,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         String user,
                         String password) throws SqlException {
        super(netLogWriter, user, password, dataSource);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        setDeferredResetPassword(password);
    }

    // For jdbc 1 connections
    public NetConnection(NetLogWriter netLogWriter,
                         int driverManagerLoginTimeout,
                         String serverName,
                         int portNumber,
                         String databaseName,
                         java.util.Properties properties) throws SqlException {
        super(netLogWriter, driverManagerLoginTimeout, serverName, portNumber, databaseName, properties);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        netAgent_ = (NetAgent) super.agent_;
        if (netAgent_.exceptionOpeningSocket_ != null) {
            throw netAgent_.exceptionOpeningSocket_;
        }
        checkDatabaseName();
        String password = ClientBaseDataSource.getPassword(properties);
        securityMechanism_ = ClientBaseDataSource.getSecurityMechanism(properties);
        flowConnect(password, securityMechanism_);
        if(!isConnectionNull())
        	completeConnect();
    }

    // For JDBC 2 Connections
    public NetConnection(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         int rmId,
                         boolean isXAConn) throws SqlException {
        super(netLogWriter, user, password, isXAConn, dataSource);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        netAgent_ = (NetAgent) super.agent_;
        initialize(password, dataSource, rmId, isXAConn);
    }

    public NetConnection(NetLogWriter netLogWriter,
                         String ipaddr,
                         int portNumber,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         boolean isXAConn) throws SqlException {
        super(netLogWriter, isXAConn, dataSource);
        this.pooledConnection_ = null;
        this.closeStatementsOnClose = true;
        netAgent_ = (NetAgent) super.agent_;
        if (netAgent_.exceptionOpeningSocket_ != null) {
            throw netAgent_.exceptionOpeningSocket_;
        }
        checkDatabaseName();
        this.isXAConnection_ = isXAConn;
        flowSimpleConnect();
        productID_ = targetSrvrlslv_;
        super.completeConnect();
    }
    
    // For JDBC 2 Connections
    /**
     * This constructor is called from the ClientPooledConnection object 
     * to enable the NetConnection to pass <code>this</code> on to the associated 
     * prepared statement object thus enabling the prepared statement object 
     * to inturn  raise the statement events to the ClientPooledConnection object
     * @param netLogWriter NetLogWriter object associated with this connection
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
    
    public NetConnection(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                         int rmId,
                         boolean isXAConn,
                         ClientPooledConnection cpc) throws SqlException {
        super(netLogWriter, user, password, isXAConn, dataSource);
        netAgent_ = (NetAgent) super.agent_;
        initialize(password, dataSource, rmId, isXAConn);
        this.pooledConnection_=cpc;
        this.closeStatementsOnClose = !cpc.isStatementPoolingEnabled();
    }

    private void initialize(String password,
                            org.apache.derby.jdbc.ClientBaseDataSource dataSource,
                            int rmId,
                            boolean isXAConn) throws SqlException {
        securityMechanism_ = dataSource.getSecurityMechanism(password);

        setDeferredResetPassword(password);
        checkDatabaseName();
        dataSource_ = dataSource;
        this.rmId_ = rmId;
        this.isXAConnection_ = isXAConn;
        flowConnect(password, securityMechanism_);
        // it's possible that the internal Driver.connect() calls returned null,
        // thus, a null connection, e.g. when the databasename has a : in it
        // (which the InternalDriver assumes means there's a subsubprotocol)  
        // and it's not a subsubprotocol recognized by our drivers.
        // If so, bail out here.
        if(!isConnectionNull())
            completeConnect();
        else
        {
            agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_,
                    new ClientMessageId(SQLState.PROPERTY_INVALID_VALUE),
                    Attribute.DBNAME_ATTR,databaseName_));
        }
    }

    // preferably without password in the method signature.
    // We can probally get rid of flowReconnect method.
    public void resetNetConnection(org.apache.derby.client.am.LogWriter logWriter)
            throws SqlException {
        super.resetConnection(logWriter);
        //----------------------------------------------------
        // do not reset managers on a connection reset.  this information shouldn't
        // change and can be used to check secmec support.

        targetExtnam_ = null;
        targetSrvclsnm_ = null;
        targetSrvnam_ = null;
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
    }


    protected void reset_(org.apache.derby.client.am.LogWriter logWriter)
            throws SqlException {
        if (inUnitOfWork_) {
            throw new SqlException(logWriter, 
                new ClientMessageId(
                    SQLState.NET_CONNECTION_RESET_NOT_ALLOWED_IN_UNIT_OF_WORK));
        }
        resetNetConnection(logWriter);
    }

    java.util.List getSpecialRegisters() {
        if (xares_ != null) {
            return xares_.getSpecialRegisters();
        } else {
            return null;
        }
    }

    public void addSpecialRegisters(String s) {
        if (xares_ != null) {
            xares_.addSpecialRegisters(s);
        }
    }

    public void completeConnect() throws SqlException {
        super.completeConnect();
    }

    protected void completeReset(boolean isDeferredReset)
            throws SqlException {
        super.completeReset(isDeferredReset, closeStatementsOnClose, xares_);
    }

    public void flowConnect(String password,
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
                    new Integer(securityMechanism));
            }
        } catch (java.lang.Throwable e) { // if *anything* goes wrong, make sure the connection is destroyed
            // always mark the connection closed in case of an error.
            // This prevents attempts to use this closed connection
            // to retrieve error message text if an error SQLCA
            // is returned in one of the connect flows.
            open_ = false;
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
                    e.getClass().getName(), e.getMessage(), e);
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
    
    protected void flowSimpleConnect() throws SqlException {
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
        } catch (java.lang.Throwable e) { // if *anything* goes wrong, make sure the connection is destroyed
            // always mark the connection closed in case of an error.
            // This prevents attempts to use this closed connection
            // to retrieve error message text if an error SQLCA
            // is returned in one of the connect flows.
            open_ = false;
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
                    e.getClass().getName(), e.getMessage(), e);
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

    protected boolean flowReconnect(String password, int securityMechanism) throws SqlException {
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
                    new Integer(securityMechanism));
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

    protected void finalize() throws java.lang.Throwable {
        super.finalize();
    }

    protected byte[] getCnnToken() {
        return cnntkn_;
    }

    protected short getSequenceNumber() {
        return ++seqNo_;
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
                targetRsyncmgr_);
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

    private void flowServerAttributesAndSeedExchange(int securityMechanism,
                                                     byte[] sourceSeed) throws SqlException {
        agent_.beginWriteChainOutsideUOW();
        writeServerAttributesAndSeedExchange(sourceSeed);
        agent_.flowOutsideUOW();
        readServerAttributesAndSeedExchange();
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
                targetRsyncmgr_);
        netAgent_.netConnectionRequest_.writeAccessSecurity(securityMechanism,
                databaseName_,
                publicKey);
    }

    private void writeServerAttributesAndSeedExchange(byte[] sourceSeed)
                                                        throws SqlException {
        
        // For now, we're just calling our cousin method to do the job
        writeServerAttributesAndKeyExchange(NetConfiguration.SECMEC_USRSSBPWD,
                                            sourceSeed);
    }

    private void readServerAttributesAndKeyExchange(int securityMechanism) throws SqlException {
        netAgent_.netConnectionReply_.readExchangeServerAttributes(this);
        netAgent_.netConnectionReply_.readAccessSecurity(this, securityMechanism);
    }

    private void readServerAttributesAndSeedExchange() throws SqlException {
        // For now, we're just calling our cousin method to do the job
        readServerAttributesAndKeyExchange(NetConfiguration.SECMEC_USRSSBPWD);
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
                prddta_,
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

    void setServerAttributeData(String extnam,
                                String srvclsnm,
                                String srvnam,
                                String srvrlslv) {
        targetExtnam_ = extnam;          // any of these could be null
        targetSrvclsnm_ = srvclsnm;      // since then can be optionally returned from the
        targetSrvnam_ = srvnam;          // server
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

    protected org.apache.derby.client.am.Agent newAgent_(org.apache.derby.client.am.LogWriter logWriter, int loginTimeout, String serverName, int portNumber, int clientSSLMode)
            throws SqlException {
        return new NetAgent(this,
                (NetLogWriter) logWriter,
                loginTimeout,
                serverName,
                portNumber,
                clientSSLMode);
    }


    protected Statement newStatement_(int type, int concurrency, int holdability) throws SqlException {
        return new NetStatement(netAgent_, this, type, concurrency, holdability).statement_;
    }

    protected void resetStatement_(Statement statement, int type, int concurrency, int holdability) throws SqlException {
        ((NetStatement) statement.materialStatement_).resetNetStatement(netAgent_, this, type, concurrency, holdability);
    }

    protected PreparedStatement newPositionedUpdatePreparedStatement_(String sql,
                                                                      org.apache.derby.client.am.Section section) throws SqlException {
        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetPreparedStatement(netAgent_, this, sql, section,pooledConnection_).preparedStatement_;
    }

    protected PreparedStatement newPreparedStatement_(String sql, int type, int concurrency, int holdability, int autoGeneratedKeys, String[] columnNames,
            int[] columnIndexes) throws SqlException {
        
        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetPreparedStatement(netAgent_, this, sql, type, concurrency, holdability, autoGeneratedKeys, columnNames,
                columnIndexes, pooledConnection_).preparedStatement_;
    }

    protected void resetPreparedStatement_(PreparedStatement ps,
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


    protected CallableStatement newCallableStatement_(String sql, int type, int concurrency, int holdability) throws SqlException {
        //passing the pooledConnection_ object which will be used to raise 
        //StatementEvents to the PooledConnection
        return new NetCallableStatement(netAgent_, this, sql, type, concurrency, holdability,pooledConnection_).callableStatement_;
    }

    protected void resetCallableStatement_(CallableStatement cs,
                                           String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) throws SqlException {
        ((NetCallableStatement) cs.materialCallableStatement_).resetNetCallableStatement(netAgent_, this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }


    protected DatabaseMetaData newDatabaseMetaData_() {
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
                new Integer(usridLength), 
                new Integer(NetConfiguration.USRID_MAXSIZE));
        }
    }

    private void checkPasswordLength(String password) throws SqlException {
        int passwordLength = password.length();
        if ((passwordLength == 0) || (passwordLength > NetConfiguration.PASSWORD_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.CONNECT_PASSWORD_LENGTH_OUT_OF_RANGE),
                new Integer(passwordLength),
                new Integer(NetConfiguration.PASSWORD_MAXSIZE));
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
        int[] supportedSecmecs = null;

        // Point to a list (array) of supported security mechanisms.
        supportedSecmecs = NetConfiguration.SECMGR_SECMECS;

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
                new Integer(securityMechanism));
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
        byte[] localAddressBytes = null;
        long time = 0;
        int num = 0;
        int halfByte = 0;
        int i = 0;
        int j = 0;

        // allocate the crrtkn array.
        if (crrtkn_ == null) {
            crrtkn_ = new byte[19];
        } else {
            java.util.Arrays.fill(crrtkn_, (byte) 0);
        }

        localAddressBytes = netAgent_.socket_.getLocalAddress().getAddress();

        // IP addresses are returned in a 4 byte array.
        // Obtain the character representation of each half byte.
        for (i = 0, j = 0; i < 4; i++, j += 2) {

            // since a byte is signed in java, convert any negative
            // numbers to positive before shifting.
            num = localAddressBytes[i] < 0 ? localAddressBytes[i] + 256 : localAddressBytes[i];
            halfByte = (num >> 4) & 0x0f;

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
        num = netAgent_.socket_.getLocalPort();

        halfByte = (num >> 12) & 0x0f;
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
        time = System.currentTimeMillis();

        for (i = 0; i < 6; i++) {
            // store 6 bytes of 8 byte time into crrtkn
            crrtkn_[i + 13] = (byte) (time >>> (40 - (i * 8)));
        }
    }


    private void constructExtnam() throws SqlException {
        /* Construct the EXTNAM based on the thread name */
        char[] chars = java.lang.Thread.currentThread().getName().toCharArray();

        /* DERBY-4584: Replace non-EBCDIC characters (> 0xff) with '?' */
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] > 0xff) chars[i] = '?';
        }
        extnam_ = "derbydnc" + new String(chars);
    }

    private void constructPrddta() throws SqlException {
        int prddtaLen = 1;

        if (prddta_ == null) {
            prddta_ = new byte[NetConfiguration.PRDDTA_MAXSIZE];
        } else {
            java.util.Arrays.fill(prddta_, (byte) 0);
        }

        for (int i = 0; i < NetConfiguration.PRDDTA_ACCT_SUFFIX_LEN_BYTE; i++) {
            prddta_[i] = netAgent_.getCurrentCcsidManager().space_;
        }

        prddtaLen = netAgent_.getCurrentCcsidManager().convertFromJavaString(NetConfiguration.PRDID,
                prddta_,
                prddtaLen,
                netAgent_);

        prddtaLen = netAgent_.getCurrentCcsidManager().convertFromJavaString(NetConfiguration.PRDDTA_PLATFORM_ID,
                prddta_,
                prddtaLen,
                netAgent_);

        int extnamTruncateLength = Math.min(extnam_.length(), NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN);
        netAgent_.getCurrentCcsidManager().convertFromJavaString(extnam_.substring(0, extnamTruncateLength),
                prddta_,
                prddtaLen,
                netAgent_);
        prddtaLen += NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN;

        if (user_ != null) {
            int userTruncateLength = Math.min(user_.length(), NetConfiguration.PRDDTA_USER_ID_FIXED_LEN);
            netAgent_.getCurrentCcsidManager().convertFromJavaString(user_.substring(0, userTruncateLength),
                    prddta_,
                    prddtaLen,
                    netAgent_);
        }

        prddtaLen += NetConfiguration.PRDDTA_USER_ID_FIXED_LEN;

        prddta_[NetConfiguration.PRDDTA_ACCT_SUFFIX_LEN_BYTE] = 0;
        prddtaLen++;
        // the length byte value does not include itself.
        prddta_[NetConfiguration.PRDDTA_LEN_BYTE] = (byte) (prddtaLen - 1);
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
        // (see ClientBaseDataSource.updateDataSourceValues().
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
                    dataSource_.propertyDefault_user) &&
                !dataSourceUserName.equalsIgnoreCase(
                    dataSource_.propertyDefault_user))
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

    public void setInputStream(java.io.InputStream inputStream) {
        netAgent_.setInputStream(inputStream);
    }

    public void setOutputStream(java.io.OutputStream outputStream) {
        netAgent_.setOutputStream(outputStream);
    }

    public java.io.InputStream getInputStream() {
        return netAgent_.getInputStream();
    }

    public java.io.OutputStream getOutputStream() {
        return netAgent_.getOutputStream();
    }


    public void writeTransactionStart(Statement statement) throws SqlException {
    }

    public void readTransactionStart() throws SqlException {
        super.readTransactionStart();
    }

    public void setIndoubtTransactions(java.util.Hashtable indoubtTransactions) {
        if (isXAConnection_) {
            if (indoubtTransactions_ != null) {
                indoubtTransactions_.clear();
            }
            indoubtTransactions_ = indoubtTransactions;
        }
    }

    protected void setReadOnlyTransactionFlag(boolean flag) {
        readOnlyTransaction_ = flag;
    }

    public org.apache.derby.client.am.SectionManager newSectionManager
            (String collection,
             org.apache.derby.client.am.Agent agent,
             String databaseName) {
        return new org.apache.derby.client.am.SectionManager(collection, agent, databaseName);
    }

    protected int getSocketAndInputOutputStreams(String server, int port, int clientSSLMode) {
        try {
            netAgent_.socket_ = (java.net.Socket) java.security.AccessController.doPrivileged(new OpenSocketAction(server, port, clientSSLMode));
        } catch (java.security.PrivilegedActionException e) {
            Exception openSocketException = e.getException();
            if (netAgent_.loggingEnabled()) {
                netAgent_.logWriter_.tracepoint("[net]", 101, "Client Re-route: " + openSocketException.getClass().getName() + " : " + openSocketException.getMessage());
            }
            return -1;
        }

        try {
            netAgent_.rawSocketOutputStream_ = netAgent_.socket_.getOutputStream();
            netAgent_.rawSocketInputStream_ = netAgent_.socket_.getInputStream();
        } catch (java.io.IOException e) {
            if (netAgent_.loggingEnabled()) {
                netAgent_.logWriter_.tracepoint("[net]", 103, "Client Re-route: java.io.IOException " + e.getMessage());
            }
            try {
                netAgent_.socket_.close();
            } catch (java.io.IOException doNothing) {
            }
            return -1;
        }
        return 0;
    }

    protected int checkAlternateServerHasEqualOrHigherProductLevel(ProductLevel orgLvl, int orgServerType) {
        if (orgLvl == null && orgServerType == 0) {
            return 0;
        }
        ProductLevel alternateServerProductLvl =
                netAgent_.netConnection_.databaseMetaData_.productLevel_;
        boolean alternateServerIsEqualOrHigherToOriginalServer =
                (alternateServerProductLvl.greaterThanOrEqualTo
                (orgLvl.versionLevel_,
                        orgLvl.releaseLevel_,
                        orgLvl.modificationLevel_)) ? true : false;
        // write an entry to the trace
        if (!alternateServerIsEqualOrHigherToOriginalServer &&
                netAgent_.loggingEnabled()) {
            netAgent_.logWriter_.tracepoint("[net]",
                    99,
                    "Client Re-route failed because the alternate server is on a lower product level than the origianl server.");
        }
        return (alternateServerIsEqualOrHigherToOriginalServer) ? 0 : -1;
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
        return targetPublicKey_;
    }

    public String getProductID() {
        return targetSrvclsnm_;
    }

    public void doResetNow() throws SqlException {
        if (!resetConnectionAtFirstSql_) {
            return; // reset not needed
        }
        agent_.beginWriteChainOutsideUOW();
        agent_.flowOutsideUOW();
        agent_.endReadChain();
    }
    
	/**
	 * @return Returns the connectionNull.
	 */
	public boolean isConnectionNull() {
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
    public final boolean serverSupportsQryclsimp() {
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;
        return metadata.serverSupportsQryclsimp();
    }

    
    public final boolean serverSupportsLayerBStreaming() {
        
        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;
        
        return metadata.serverSupportsLayerBStreaming();

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
     * Check whether the server supports boolean values
     * @return true if boolean values are supported
     */
    protected final boolean serverSupportsBooleanValues() {

        NetDatabaseMetaData metadata =
            (NetDatabaseMetaData) databaseMetaData_;

        return metadata.serverSupportsBooleanValues();
    }

    /**
     * Returns if a transaction is in process
     * @return open
     */
    public boolean isOpen() {
        return open_;
    }
    
    /**
     * closes underlying connection and associated resource.
     */
    synchronized public void close() throws SQLException {
        // call super.close*() to do the close*
        super.close();
        if (!isXAConnection_)
            return;
        if (isOpen()) {
            return; // still open, return
        }
    }
    
    /**
     * closes underlying connection and associated resource.
     */
    synchronized public void closeX() throws SQLException {
        // call super.close*() to do the close*
        super.closeX();
        if (!isXAConnection_)
            return;
        if (isOpen()) {
            return; // still open, return
        }
    }
    
    /**
     * Invalidates connection but keeps socket open.
     */
    synchronized public void closeForReuse() throws SqlException {
        // call super.close*() to do the close*
        super.closeForReuse(closeStatementsOnClose);
        if (!isXAConnection_)
            return;
        if (isOpen()) {
            return; // still open, return
        }
    }
    
    /**
     * closes resources connection will be not available 
     * for reuse.
     */
    synchronized public void closeResources() throws SQLException {
        // call super.close*() to do the close*
        super.closeResources();
        if (!isXAConnection_)
            return;
        
        if (isOpen()) {
            return; // still open, return
        }
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
    
    
    protected void writeXATransactionStart(Statement statement) throws SqlException {
        xares_.netXAConn_.writeTransactionStart(statement);
    }
}

