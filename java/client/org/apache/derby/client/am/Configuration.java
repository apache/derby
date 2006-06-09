/*

   Derby - Class org.apache.derby.client.am.Configuration

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

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.info.JVMInfo;

public class Configuration {


    public static int traceFileSuffixIndex__ = 0;

    public static int traceLevel__ = org.apache.derby.jdbc.ClientBaseDataSource.TRACE_ALL;

    public static String traceFile__ = null;

    public static String traceDirectory__ = null;

    public static boolean traceFileAppend__ = false;
    public static String jreLevel = "1.3.0"; // default level if unable to read
    public static int jreLevelMajor = 1;
    public static int jreLevelMinor = 3;

    private Configuration() {
    }

    public static boolean traceSuspended__;

    public static boolean[] enableConnectivityToTargetServer__;
    public static boolean jvmSupportsMicrosClock__ = false;

    // -------------------------- versioning -------------------------------------

    public static ProductVersionHolder dncProductVersionHolder__;

    public static ProductVersionHolder getProductVersionHolder() {
        return dncProductVersionHolder__;
    }


    // for DatabaseMetaData.getDriverName()
    public final static String dncDriverName = "Apache Derby Network Client JDBC Driver";


    // Hard-wired for JDBC
    //
    // Currently ASCII hex value of "SYSLVL01".
    public final static byte[] dncPackageConsistencyToken =
            {0x53, 0x59, 0x53, 0x4c, 0x56, 0x4c, 0x30, 0x31};

    // We will not set packagge VERSION in the initial release.
    // If we have to change the package version in the future then we can.
    public static String dncPackageVersion = null;

    // for Driver.jdbcCompliant()
    public final static boolean jdbcCompliant = true;

    // for Driver.getCompatibileJREVersions()
    public final static String[] dncCompatibleJREVersions = new String[]{"1.3", "1.4"};

    //---------------------- database URL protocols ------------------------------

    // For DatabaseMetaData.getURL()
    public final static String jdbcDerbyNETProtocol = "jdbc:derby://";

    // -------------------------- metrics ----------------------
    // Not currently used by production builds.
    // We can't really use this stuff with tracing enabled, the results are not accurate.

    // -------------------------- compiled in properties -------------------------

    public final static boolean enableNetConnectionPooling = true;

    final static boolean rangeCheckCrossConverters = true;

    // Define different levels of bug checking, for now turn all bits on.
    final static int bugCheckLevel = 0xff;

    // --------------------------- connection defaults ---------------------------

    // This is the DERBY default and maps to DERBY's "Cursor Stability".
    public final static int defaultIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    // ---------------------------- statement defaults----------------------------

    public static final int defaultFetchSize = 64;

    // Prepare attribute constants
    public static final String cursorAttribute_SensitiveStatic = "SENSITIVE STATIC SCROLL ";
    public static final String cursorAttribute_SensitiveStaticRowset = cursorAttribute_SensitiveStatic;
    public static final String cursorAttribute_SensitiveDynamic = "SENSITIVE DYNAMIC SCROLL ";
    public static final String cursorAttribute_SensitiveDynamicRowset = "SENSITIVE DYNAMIC SCROLL WITH ROWSET POSITIONING ";
    public static final String cursorAttribute_Insensitive = "INSENSITIVE SCROLL ";
    public static final String cursorAttribute_InsensitiveRowset = cursorAttribute_Insensitive;

    // uncomment the following when we want to use multi-row fetch to support sensitive static and
    // insensitve cursors whenever the server has support for it.
    //public static final String cursorAttribute_SensitiveStaticRowset = "SENSITIVE STATIC SCROLL WITH ROWSET POSITIONING ";
    //public static final String cursorAttribute_InsensitiveRowset = "INSENSITIVE SCROLL WITH ROWSET POSITIONING ";

    public static final String cursorAttribute_ForUpdate = "FOR UPDATE ";
    public static final String cursorAttribute_ForReadOnly = "FOR READ ONLY ";

    public static final String cursorAttribute_WithHold = "WITH HOLD ";

    // -----------------------Load resource bundles for the driver asap-----------

    private static final String packageNameForDNC = "org.apache.derby.client";

    public static SqlException exceptionsOnLoadResources = null; // used by ClientDriver to accumulate load exceptions

    static {
        try {
            loadProductVersionHolder();
        } catch (SqlException e) {
            exceptionsOnLoadResources = e;
        }
        try {
            jreLevel = System.getProperty("java.version");
        } catch (SecurityException e) {
        } // ignore it, assume 1.3.0
        java.util.StringTokenizer st = new java.util.StringTokenizer(jreLevel, ".");
        int jreState = 0;
        while (st.hasMoreTokens()) {
            int i;
            try {
                i = java.lang.Integer.parseInt(st.nextToken()); // get int value
            } catch (NumberFormatException e) {
                i = 0;
            }
            switch (jreState++) {
            case 0:
                jreLevelMajor = i; // state 0, this is the major version
                break;
            case 1:
                jreLevelMinor = i; // state 1, this is the minor version
                break;
            default:
                break; // state >1, ignore
            }
        }
    }

    /**
     * load product version information and accumulate exceptions
     */
    private static void loadProductVersionHolder() throws SqlException {
        try {
            dncProductVersionHolder__ = buildProductVersionHolder();
        } catch (java.security.PrivilegedActionException e) {
            throw new SqlException(null, 
                    new ClientMessageId (SQLState.ERROR_PRIVILEGED_ACTION),
                    e.getException());                    
        } catch (java.io.IOException ioe) {
            throw SqlException.javaException(null, ioe);
        }
    }


    // Create ProductVersionHolder in security block for Java 2 security.
    private static ProductVersionHolder buildProductVersionHolder() throws
            java.security.PrivilegedActionException, IOException {
        ProductVersionHolder myPVH = null;
        myPVH = (ProductVersionHolder)
                AccessController.doPrivileged(new PrivilegedExceptionAction() {

                    public Object run() throws IOException {
                        InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.DNC_INFO);

                        return ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);
                    }
                });

        return myPVH;
    }
    
    /**
     * Check to see if the jvm version is such that JDBC 4.0 is supported
     */
    
    public static boolean supportsJDBC40() {
        if (JVMInfo.JDK_ID >= JVMInfo.J2SE_16) {
            return true;
        }
        return false;
    }



}
