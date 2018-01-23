/*

   Derby - Class org.apache.derby.client.am.Configuration

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

package org.apache.derby.client.am;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;

import org.apache.derby.shared.common.info.ProductGenusNames;
import org.apache.derby.shared.common.info.ProductVersionHolder;
import org.apache.derby.shared.common.reference.SQLState;

public class Configuration {

    private Configuration() {
    }

    // -------------------------- versioning -------------------------------------

    private static ProductVersionHolder dncProductVersionHolder__;

    static ProductVersionHolder getProductVersionHolder() {
        return dncProductVersionHolder__;
    }


    // for DatabaseMetaData.getDriverName()
    final static String
        dncDriverName = "Apache Derby Network Client JDBC Driver";


    // Hard-wired for JDBC
    //
    // Currently ASCII hex value of "SYSLVL01".
    private final static byte[] dncPackageConsistencyToken =
            {0x53, 0x59, 0x53, 0x4c, 0x56, 0x4c, 0x30, 0x31};

    public static byte[] getDncPackageConsistencyToken() {
        return dncPackageConsistencyToken.clone();
    }

    // for ClientAutoloadedDriver.jdbcCompliant()
    public final static boolean jdbcCompliant = true;

    private final static String[] dncCompatibleJREVersions =
            {"1.5", "1.6", "1.7", "1.8"};

    static String[] getDncCompatibleJREVersions() {
        return dncCompatibleJREVersions.clone();
    }

    //---------------------- database URL protocols ------------------------------

    // For DatabaseMetaData.getURL()
    public final static String jdbcDerbyNETProtocol = "jdbc:derby://";

    // -------------------------- metrics ----------------------
    // Not currently used by production builds.
    // We can't really use this stuff with tracing enabled, the results are not accurate.

    // -------------------------- compiled in properties -------------------------

    final static boolean rangeCheckCrossConverters = true;

    // Define different levels of bug checking, for now turn all bits on.
    final static int bugCheckLevel = 0xff;

    // --------------------------- connection defaults ---------------------------

    // This is the DERBY default and maps to DERBY's "Cursor Stability".
    final static int defaultIsolation = Connection.TRANSACTION_READ_COMMITTED;

    // ---------------------------- statement defaults----------------------------

    public static final int defaultFetchSize = 64;

    // Prepare attribute constants
    static final String
            cursorAttribute_SensitiveStatic = "SENSITIVE STATIC SCROLL ";
    static final String
            cursorAttribute_Insensitive = "INSENSITIVE SCROLL ";
    static final String
            cursorAttribute_ForUpdate = "FOR UPDATE ";
    static final String
            cursorAttribute_WithHold = "WITH HOLD ";

    // -----------------------Load resource bundles for the driver asap-----------

    /**
     * Used by ClientAutoloadedDriver to accumulate load exceptions
     */
    private static SqlException exceptionsOnLoadResources = null;

    static {
        try {
            loadProductVersionHolder();
        } catch (SqlException e) {
            exceptionsOnLoadResources = e;
        }
    }

    public static SqlException getExceptionOnLoadResources() {
        return exceptionsOnLoadResources;
    }
    
    /**
     * load product version information and accumulate exceptions
     */
    private static void loadProductVersionHolder() throws SqlException {
        try {
            dncProductVersionHolder__ = buildProductVersionHolder();
        } catch (PrivilegedActionException e) {
            throw new SqlException(null, 
                    new ClientMessageId (SQLState.ERROR_PRIVILEGED_ACTION),
                    e.getException());                    
        } catch (IOException ioe) {
            throw SqlException.javaException(null, ioe);
        }
    }


    // Create ProductVersionHolder in security block for Java 2 security.
    private static ProductVersionHolder buildProductVersionHolder() throws
            PrivilegedActionException, IOException {
        return AccessController.doPrivileged(
                new PrivilegedExceptionAction<ProductVersionHolder>() {

                    public ProductVersionHolder run() throws IOException {
                        InputStream versionStream = getClass().getResourceAsStream("/" + ProductGenusNames.CLIENT_INFO);

                        return ProductVersionHolder.getProductVersionHolderFromMyEnv(versionStream);
                    }
                });
    }
    
    /**
     * Check to see if the jvm version is such that JDBC 4.2 is supported
     */
    
    public static boolean supportsJDBC42() {
        // use reflection to identify whether we support JDBC42
        try {
            Class.forName("java.sql.SQLType");
            return true;
        } catch (Exception e) {
            return false;
        }
    }



}
