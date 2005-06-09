/*

   Derby - Class org.apache.derby.client.net.NetConnection

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

import org.apache.derby.jdbc.ClientBaseDataSource;
import org.apache.derby.client.am.CallableStatement;
import org.apache.derby.client.am.DatabaseMetaData;
import org.apache.derby.client.am.DisconnectException;
import org.apache.derby.client.am.EncryptionManager;
import org.apache.derby.client.am.PreparedStatement;
import org.apache.derby.client.am.ProductLevel;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Statement;
import org.apache.derby.client.am.Utils;
import org.apache.derby.jdbc.ClientDataSource;

public class NetConnection extends org.apache.derby.client.am.Connection {

    protected NetAgent netAgent_;


    // For XA Transaction
    protected int pendingEndXACallinfoOffset_ = -1;


    // byte[] to save the connect flows for connection reset
    protected byte[] cachedConnectBytes_ = null;
    protected boolean wroteConnectFromCache_ = false;
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
    }

    public NetConnection(NetLogWriter netLogWriter,
                         org.apache.derby.jdbc.ClientDataSource dataSource,
                         String user,
                         String password) throws SqlException {
        super(netLogWriter, user, password, dataSource);
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
        netAgent_ = (NetAgent) super.agent_;
        if (netAgent_.exceptionOpeningSocket_ != null) {
            throw netAgent_.exceptionOpeningSocket_;
        }
        checkDatabaseName();
        String password = ClientDataSource.getPassword(properties);
        securityMechanism_ = ClientDataSource.getSecurityMechanism(properties);
        flowConnect(password, securityMechanism_);
        completeConnect();
    }

    // For JDBC 2 Connections
    public NetConnection(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         org.apache.derby.jdbc.ClientDataSource dataSource,
                         int rmId,
                         boolean isXAConn) throws SqlException {
        super(netLogWriter, user, password, isXAConn, dataSource);
        netAgent_ = (NetAgent) super.agent_;
        initialize(user, password, dataSource, rmId, isXAConn);
    }

    public NetConnection(NetLogWriter netLogWriter,
                         String ipaddr,
                         int portNumber,
                         org.apache.derby.jdbc.ClientDataSource dataSource,
                         boolean isXAConn) throws SqlException {
        super(netLogWriter, isXAConn, dataSource);
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

    private void initialize(String user,
                            String password,
                            org.apache.derby.jdbc.ClientDataSource dataSource,
                            int rmId,
                            boolean isXAConn) throws SqlException {
        securityMechanism_ = dataSource.getSecurityMechanism();

        setDeferredResetPassword(password);
        checkDatabaseName();
        dataSource_ = dataSource;
        this.rmId_ = rmId;
        this.isXAConnection_ = isXAConn;
        flowConnect(password, securityMechanism_);
        completeConnect();

    }

    // preferably without password in the method signature.
    // We can probally get rid of flowReconnect method.
    public void resetNetConnection(org.apache.derby.client.am.LogWriter logWriter,
                                   String user,
                                   String password,
                                   org.apache.derby.jdbc.ClientDataSource ds,
                                   boolean recomputeFromDataSource) throws SqlException {
        super.resetConnection(logWriter, user, ds, recomputeFromDataSource);
        //----------------------------------------------------
        if (recomputeFromDataSource) {
            // do not reset managers on a connection reset.  this information shouldn't
            // change and can be used to check secmec support.

            targetExtnam_ = null;
            targetSrvclsnm_ = null;
            targetSrvnam_ = null;
            targetSrvrlslv_ = null;
            publicKey_ = null;
            targetPublicKey_ = null;
            targetSecmec_ = 0;
            if (ds != null && securityMechanism_ == 0) {
                securityMechanism_ = ds.getSecurityMechanism();
            }
            resetConnectionAtFirstSql_ = false;
            if (resultSetHoldability_ == 0) {
                ((org.apache.derby.client.net.NetDatabaseMetaData) databaseMetaData_).setDefaultResultSetHoldability();
            }
        }
        if (password != null) {
            deferredResetPassword_ = null;
        } else {
            password = getDeferredResetPassword();
        }
        // properties prddta_ and crrtkn_ will be initialized by
        // calls to constructPrddta() and constructCrrtkn()
        //----------------------------------------------------------
        boolean isDeferredReset = flowReconnect(password, securityMechanism_);
        completeReset(isDeferredReset, recomputeFromDataSource);
    }


    protected void reset_(org.apache.derby.client.am.LogWriter logWriter,
                          String user, String password,
                          ClientDataSource ds,
                          boolean recomputeFromDataSource) throws SqlException {
        checkResetPreconditions(logWriter, user, password, ds);
        resetNetConnection(logWriter, user, password, ds, recomputeFromDataSource);
    }

    protected void reset_(org.apache.derby.client.am.LogWriter logWriter,
                          ClientDataSource ds,
                          boolean recomputeFromDataSource) throws SqlException {
        checkResetPreconditions(logWriter, null, null, ds);
        resetNetConnection(logWriter, ds, recomputeFromDataSource);
    }

    private void resetNetConnection(org.apache.derby.client.am.LogWriter logWriter,
                                    org.apache.derby.jdbc.ClientDataSource ds,
                                    boolean recomputeFromDataSource) throws SqlException {
        super.resetConnection(logWriter, null, ds, recomputeFromDataSource);
        //----------------------------------------------------
        if (recomputeFromDataSource) {
            // do not reset managers on a connection reset.  this information shouldn't
            // change and can be used to check secmec support.

            targetExtnam_ = null;
            targetSrvclsnm_ = null;
            targetSrvnam_ = null;
            targetSrvrlslv_ = null;
            publicKey_ = null;
            targetPublicKey_ = null;
            targetSecmec_ = 0;
            if (ds != null && securityMechanism_ == 0) {
                securityMechanism_ = ds.getSecurityMechanism();
            }
            resetConnectionAtFirstSql_ = false;

            if (resultSetHoldability_ == 0) {
                ((org.apache.derby.client.net.NetDatabaseMetaData) databaseMetaData_).setDefaultResultSetHoldability();
            }
        }
        // properties prddta_ and crrtkn_ will be initialized by
        // calls to constructPrddta() and constructCrrtkn()
        //----------------------------------------------------------
        boolean isDeferredReset = flowReconnect(null, securityMechanism_);
        completeReset(isDeferredReset, recomputeFromDataSource);
    }

    protected void checkResetPreconditions(org.apache.derby.client.am.LogWriter logWriter,
                                           String user,
                                           String password,
                                           ClientDataSource ds) throws SqlException {
        if (inUnitOfWork_) {
            throw new SqlException(logWriter, "Connection reset is not allowed when inside a unit of work.");
        }
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

    protected void completeReset(boolean isDeferredReset, boolean recomputeFromDataSource) throws SqlException {
        super.completeReset(isDeferredReset, recomputeFromDataSource);
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

        // If password is not null, change security mechanism from
        // SECMEC_USRIDONL to SECMEC_USRIDPWD
        securityMechanism = ClientBaseDataSource.getUpgradedSecurityMechanism((short) securityMechanism, password);
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

            default:
                throw new SqlException(agent_.logWriter_, "security mechanism '" + securityMechanism + "' not supported");
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
                exceptionToBeThrown = new SqlException(agent_.logWriter_, e, "Unexpected throwable caught " + e.toString());
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
                exceptionToBeThrown = new SqlException(agent_.logWriter_, e, "Unexpected throwable caught " + e.toString());
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
            default:
                throw new SqlException(agent_.logWriter_, "security mechanism '" + securityMechanism + "' not supported");
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
                targetRsyncmgr_);
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
                prddta_,
                netAgent_.typdef_);
    }

    private void cacheConnectBytes(int beginOffset, int endOffset) {
        int length = endOffset - beginOffset;
        cachedConnectBytes_ = new byte[length];
        netAgent_.netConnectionRequest_.finalizePreviousChainedDss(false);
        System.arraycopy(netAgent_.netConnectionRequest_.bytes_,
                beginOffset,
                cachedConnectBytes_,
                0,
                length);
        netAgent_.netConnectionRequest_.setDssLengthLocation(netAgent_.netConnectionRequest_.offset_);
    }

    private void readSecurityCheckAndAccessRdb() throws SqlException {
        netAgent_.netConnectionReply_.readSecurityCheck(this);
        netAgent_.netConnectionReply_.readAccessDatabase(this);
    }

    void writeDeferredReset() throws SqlException {
        if (canUseCachedConnectBytes_ && cachedConnectBytes_ != null &&
                (securityMechanism_ == NetConfiguration.SECMEC_USRIDPWD ||
                securityMechanism_ == NetConfiguration.SECMEC_USRIDONL)) {
            writeDeferredResetFromCache();
            wroteConnectFromCache_ = true;
        } else {
            int beginOffset = netAgent_.netConnectionRequest_.offset_;
            int endOffset = 0;
            // NetConfiguration.SECMEC_USRIDPWD
            if (securityMechanism_ == NetConfiguration.SECMEC_USRIDPWD) {
                writeAllConnectCommandsChained(NetConfiguration.SECMEC_USRIDPWD,
                        user_,
                        getDeferredResetPassword());
                endOffset = netAgent_.netConnectionRequest_.offset_;
                cacheConnectBytes(beginOffset, endOffset);
            }
            // NetConfiguration.SECMEC_USRIDONL
            else if (securityMechanism_ == NetConfiguration.SECMEC_USRIDONL) {
                writeAllConnectCommandsChained(NetConfiguration.SECMEC_USRIDONL,
                        user_,
                        null);  //password
                endOffset = netAgent_.netConnectionRequest_.offset_;
                cacheConnectBytes(beginOffset, endOffset);
            }
            // either NetConfiguration.SECMEC_USRENCPWD or NetConfiguration.SECMEC_EUSRIDPWD
            else {
                initializePublicKeyForEncryption();
                // Set the resetConnectionAtFirstSql_ to false to avoid going in an
                // infinite loop, since all the flow methods call beginWriteChain which then
                // calls writeDeferredResetConnection where the check for resetConnectionAtFirstSql_
                // is done. By setting the resetConnectionAtFirstSql_ to false will avoid calling the
                // writeDeferredReset method again.
                resetConnectionAtFirstSql_ = false;
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
                // NetConfiguration.SECMEC_EUSRIDPWD
                else {
                    writeSecurityCheckAndAccessRdb(NetConfiguration.SECMEC_EUSRIDPWD,
                            null, //user
                            null, //password
                            encryptedUseridForEUSRIDPWD(),
                            encryptedPasswordForEUSRIDPWD(getDeferredResetPassword()));
                }
            }
        }
    }

    void readDeferredReset() throws SqlException {
        resetConnectionAtFirstSql_ = false;
        if (wroteConnectFromCache_) {
            netAgent_.netConnectionReply_.verifyDeferredReset();
            return;
        }
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
                        (targetSecmec_ == NetConfiguration.SECMEC_EUSRIDDTA) ||
                        (targetSecmec_ == NetConfiguration.SECMEC_EUSRPWDDTA)) {

                    // a security token is required for USRENCPWD, or EUSRIDPWD.
                    if (!sectknReceived) {
                        agent_.accumulateChainBreakingReadExceptionAndThrow(new DisconnectException(agent_, "secktn was not returned "));
                    } else {
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
                agent_.accumulateReadException(new SqlException(agent_.logWriter_, "secmec not supported"));
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

    protected org.apache.derby.client.am.Agent newAgent_(org.apache.derby.client.am.LogWriter logWriter, int loginTimeout, String serverName, int portNumber)
            throws SqlException {
        return new NetAgent(this,
                (NetLogWriter) logWriter,
                loginTimeout,
                serverName,
                portNumber);
    }


    protected Statement newStatement_(int type, int concurrency, int holdability) throws SqlException {
        return new NetStatement(netAgent_, this, type, concurrency, holdability).statement_;
    }

    protected void resetStatement_(Statement statement, int type, int concurrency, int holdability) throws SqlException {
        ((NetStatement) statement.materialStatement_).resetNetStatement(netAgent_, this, type, concurrency, holdability);
    }

    protected PreparedStatement newPositionedUpdatePreparedStatement_(String sql,
                                                                      org.apache.derby.client.am.Section section) throws SqlException {
        return new NetPreparedStatement(netAgent_, this, sql, section).preparedStatement_;
    }

    protected PreparedStatement newPreparedStatement_(String sql, int type, int concurrency, int holdability, int autoGeneratedKeys, String[] columnNames) throws SqlException {
        return new NetPreparedStatement(netAgent_, this, sql, type, concurrency, holdability, autoGeneratedKeys, columnNames).preparedStatement_;
    }

    protected void resetPreparedStatement_(PreparedStatement ps,
                                           String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability,
                                           int autoGeneratedKeys,
                                           String[] columnNames) throws SqlException {
        ((NetPreparedStatement) ps.materialPreparedStatement_).resetNetPreparedStatement(netAgent_, this, sql, resultSetType, resultSetConcurrency, resultSetHoldability, autoGeneratedKeys, columnNames);
    }


    protected CallableStatement newCallableStatement_(String sql, int type, int concurrency, int holdability) throws SqlException {
        return new NetCallableStatement(netAgent_, this, sql, type, concurrency, holdability).callableStatement_;
    }

    protected void resetCallableStatement_(CallableStatement cs,
                                           String sql,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) throws SqlException {
        ((NetCallableStatement) cs.materialCallableStatement_).resetNetCallableStatement(netAgent_, this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }


    protected DatabaseMetaData newDatabaseMetaData_() {
        return new NetDatabaseMetaData(netAgent_, this);
    }

    //-------------------private helper methods--------------------------------

    private void checkDatabaseName() throws SqlException {
        // netAgent_.logWriter may not be initialized yet
        if (databaseName_ == null) {
            throw new SqlException(agent_.logWriter_, "Required property \"databaseName\" not set");
        }
    }

    private void checkUserLength(String user) throws SqlException {
        int usridLength = user.length();
        if ((usridLength == 0) || (usridLength > NetConfiguration.USRID_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_, "userid length, " + usridLength + ", is not allowed.");
        }
    }

    private void checkPasswordLength(String password) throws SqlException {
        int passwordLength = password.length();
        if ((passwordLength == 0) || (passwordLength > NetConfiguration.PASSWORD_MAXSIZE)) {
            throw new SqlException(netAgent_.logWriter_, "password length, " + passwordLength + ", is not allowed.");
        }
    }

    private void checkUser(String user) throws SqlException {
        if (user == null) {
            throw new SqlException(netAgent_.logWriter_, "null userid not supported");
        }
        checkUserLength(user);
    }

    private void checkUserPassword(String user, String password) throws SqlException {
        checkUser(user);
        if (password == null) {
            throw new SqlException(netAgent_.logWriter_, "null password not supported");
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
            throw new SqlException(agent_.logWriter_, "Security mechananism specified by app not supported by connection");
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
                    "Connection authorization failure occurred.  Reason: security mechanism not supported"); //"08001", -30082);
        case CodePoint.SECCHKCD_10:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: password missing.");
        case CodePoint.SECCHKCD_12:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: userid missing.");
        case CodePoint.SECCHKCD_13:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: userid invalid.");
        case CodePoint.SECCHKCD_14:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: userid revoked.");
        case CodePoint.SECCHKCD_15:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: new password invalid.");
        case CodePoint.SECCHKCD_0A:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: local security service non-retryable error.");
        case CodePoint.SECCHKCD_0B:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: SECTKN missing on ACCSEC when it is required or it is invalid");
        case CodePoint.SECCHKCD_0E:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: password expired.");
        case CodePoint.SECCHKCD_0F:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: password invalid.");
        default:  // ERROR SVRCOD
            return new SqlException(agent_.logWriter_,
                    "Connection authorization failure occurred.  Reason: not specified.");
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
                crrtkn_[j] = netAgent_.sourceCcsidManager_.numToSnaRequiredCrrtknChar_[halfByte];
            } else {
                crrtkn_[j] = netAgent_.sourceCcsidManager_.numToCharRepresentation_[halfByte];
            }

            halfByte = (num) & 0x0f;
            crrtkn_[j + 1] = netAgent_.sourceCcsidManager_.numToCharRepresentation_[halfByte];
        }

        // fill the '.' in between the IP address and the port number
        crrtkn_[8] = netAgent_.sourceCcsidManager_.dot_;

        // Port numbers have values which fit in 2 unsigned bytes.
        // Java returns port numbers in an int so the value is not negative.
        // Get the character representation by converting the
        // 4 low order half bytes to the character representation.
        num = netAgent_.socket_.getLocalPort();

        halfByte = (num >> 12) & 0x0f;
        crrtkn_[9] = netAgent_.sourceCcsidManager_.numToSnaRequiredCrrtknChar_[halfByte];
        halfByte = (num >> 8) & 0x0f;
        crrtkn_[10] = netAgent_.sourceCcsidManager_.numToCharRepresentation_[halfByte];
        halfByte = (num >> 4) & 0x0f;
        crrtkn_[11] = netAgent_.sourceCcsidManager_.numToCharRepresentation_[halfByte];
        halfByte = (num) & 0x0f;
        crrtkn_[12] = netAgent_.sourceCcsidManager_.numToCharRepresentation_[halfByte];

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
        extnam_ = "derbydnc" + java.lang.Thread.currentThread().getName();
    }

    private void constructPrddta() throws SqlException {
        int prddtaLen = 1;

        if (prddta_ == null) {
            prddta_ = new byte[NetConfiguration.PRDDTA_MAXSIZE];
        } else {
            java.util.Arrays.fill(prddta_, (byte) 0);
        }

        for (int i = 0; i < NetConfiguration.PRDDTA_ACCT_SUFFIX_LEN_BYTE; i++) {
            prddta_[i] = netAgent_.sourceCcsidManager_.space_;
        }

        prddtaLen = netAgent_.sourceCcsidManager_.convertFromUCS2(NetConfiguration.PRDID,
                prddta_,
                prddtaLen,
                netAgent_);

        prddtaLen = netAgent_.sourceCcsidManager_.convertFromUCS2(NetConfiguration.PRDDTA_PLATFORM_ID,
                prddta_,
                prddtaLen,
                netAgent_);

        int extnamTruncateLength = Utils.min(extnam_.length(), NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN);
        netAgent_.sourceCcsidManager_.convertFromUCS2(extnam_.substring(0, extnamTruncateLength),
                prddta_,
                prddtaLen,
                netAgent_);
        prddtaLen += NetConfiguration.PRDDTA_APPL_ID_FIXED_LEN;

        if (user_ != null) {
            int userTruncateLength = Utils.min(user_.length(), NetConfiguration.PRDDTA_USER_ID_FIXED_LEN);
            netAgent_.sourceCcsidManager_.convertFromUCS2(user_.substring(0, userTruncateLength),
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
            encryptionManager_ = new org.apache.derby.client.am.EncryptionManager(agent_);
        }
        publicKey_ = encryptionManager_.obtainPublicKey();
    }


    private byte[] encryptedPasswordForUSRENCPWD(String password) throws SqlException {
        return encryptionManager_.encryptData(netAgent_.sourceCcsidManager_.convertFromUCS2(password, netAgent_),
                NetConfiguration.SECMEC_USRENCPWD,
                netAgent_.sourceCcsidManager_.convertFromUCS2(user_, netAgent_),
                targetPublicKey_);
    }

    private byte[] encryptedUseridForEUSRIDPWD() throws SqlException {
        return encryptionManager_.encryptData(netAgent_.sourceCcsidManager_.convertFromUCS2(user_, netAgent_),
                NetConfiguration.SECMEC_EUSRIDPWD,
                targetPublicKey_,
                targetPublicKey_);
    }

    private byte[] encryptedPasswordForEUSRIDPWD(String password) throws SqlException {
        return encryptionManager_.encryptData(netAgent_.sourceCcsidManager_.convertFromUCS2(password, netAgent_),
                NetConfiguration.SECMEC_EUSRIDPWD,
                targetPublicKey_,
                targetPublicKey_);
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

    private void writeDeferredResetFromCache() {
        int length = cachedConnectBytes_.length;
        System.arraycopy(cachedConnectBytes_,
                0,
                netAgent_.netConnectionRequest_.bytes_,
                netAgent_.netConnectionRequest_.offset_,
                length);
        netAgent_.netConnectionRequest_.offset_ += length;
        netAgent_.netConnectionRequest_.setDssLengthLocation(netAgent_.netConnectionRequest_.offset_);
        netAgent_.netConnectionRequest_.setCorrelationID(4);
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
        return true;
    }

    protected boolean allowCloseInUOW_() {
        return false;
    }

    // Driver-specific determination if local COMMIT/ROLLBACK is allowed;
    // Allow local COMMIT/ROLLBACK only if we are not in an XA transaction
    protected boolean allowLocalCommitRollback_() throws org.apache.derby.client.am.SqlException {
       
    	if (xaState_ == XA_T0_NOT_ASSOCIATED) {
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

    protected int getSocketAndInputOutputStreams(String server, int port) {
        try {
            netAgent_.socket_ = (java.net.Socket) java.security.AccessController.doPrivileged(new OpenSocketAction(server, port));
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
}

