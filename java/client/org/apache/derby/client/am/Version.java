/*

   Derby - Class org.apache.derby.client.am.Version

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

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.derby.shared.common.i18n.MessageUtil;
import org.apache.derby.shared.common.reference.MessageId;



public abstract class Version {
    private static final MessageUtil msgutil = SqlException.getMessageUtil();
    
    // for DatabaseMetaData.getDriverVersion()
    public static String getDriverVersion() {
        return Configuration.getProductVersionHolder().getVersionBuildString(true);
    }


    // Same as java.sql.Driver.getMajorVersion(), getMinorVersion()
    public static int getMajorVersion() {
        return Configuration.getProductVersionHolder().getMajorVersion();
    }

    public static int getMinorVersion() {
        return Configuration.getProductVersionHolder().getMinorVersion();
    }

    public static int getProtocolMaintVersion() {
        return Configuration.getProductVersionHolder().getDrdaMaintVersion();
    }

    // Not an external, just a helper method
    private static String getDriverNameAndVersion() {
        return Configuration.dncDriverName + " " +
                Configuration.getProductVersionHolder().getVersionBuildString(true);
    }

    // -------------------------- configuration print stream ---------------------

    static void writeDriverConfiguration(PrintWriter printWriter) {
        String header = "[derby] ";
        synchronized (printWriter) {
            printWriter.println(header + "BEGIN TRACE_DRIVER_CONFIGURATION");
            printWriter.println(header + "Driver: " + getDriverNameAndVersion());

            printWriter.print(header + "Compatible JRE versions: { ");
            String [] cv = Configuration.getDncCompatibleJREVersions();
            for (int i = 0; i < cv.length; i++) {
                printWriter.print(cv[i]);
                if (i != cv.length - 1) {
                    printWriter.print(", ");
                }
            }
            printWriter.println(" }");

            printWriter.println(header + "Range checking enabled: " + Configuration.rangeCheckCrossConverters);
            printWriter.println(header + "Bug check level: 0x" + Integer.toHexString(Configuration.bugCheckLevel));
            printWriter.println(header + "Default fetch size: " + Configuration.defaultFetchSize);
            printWriter.println(header + "Default isolation: " + Configuration.defaultIsolation);

            SecurityManager security = System.getSecurityManager();
            if (security == null) {
                printWriter.println(header + "No security manager detected.");
            } else {
                printWriter.println(header + "Security manager detected.");
            }

            detectLocalHost(System.getSecurityManager(), printWriter);

            printSystemProperty(security, "JDBC 1 system property jdbc.drivers = ", "jdbc.drivers", printWriter);

            printSystemProperty(security, "Java Runtime Environment version ", "java.version", printWriter);
            printSystemProperty(security, "Java Runtime Environment vendor = ", "java.vendor", printWriter);
            printSystemProperty(security, "Java vendor URL = ", "java.vendor.url", printWriter);
            printSystemProperty(security, "Java installation directory = ", "java.home", printWriter);
            printSystemProperty(security, "Java Virtual Machine specification version = ", "java.vm.specification.version", printWriter);
            printSystemProperty(security, "Java Virtual Machine specification vendor = ", "java.vm.specification.vendor", printWriter);
            printSystemProperty(security, "Java Virtual Machine specification name = ", "java.vm.specification.name", printWriter);
            printSystemProperty(security, "Java Virtual Machine implementation version = ", "java.vm.version", printWriter);
            printSystemProperty(security, "Java Virtual Machine implementation vendor = ", "java.vm.vendor", printWriter);
            printSystemProperty(security, "Java Virtual Machine implementation name = ", "java.vm.name", printWriter);
            printSystemProperty(security, "Java Runtime Environment specification version = ", "java.specification.version", printWriter);
            printSystemProperty(security, "Java Runtime Environment specification vendor = ", "java.specification.vendor", printWriter);
            printSystemProperty(security, "Java Runtime Environment specification name = ", "java.specification.name", printWriter);
            printSystemProperty(security, "Java class format version number = ", "java.class.version", printWriter);
            printSystemProperty(security, "Java class path = ", "java.class.path", printWriter);
            printSystemProperty(security, "Java native library path = ", "java.library.path", printWriter);
            printSystemProperty(security, "Path of extension directory or directories = ", "java.ext.dirs", printWriter);
            printSystemProperty(security, "Operating system name = ", "os.name", printWriter);
            printSystemProperty(security, "Operating system architecture = ", "os.arch", printWriter);
            printSystemProperty(security, "Operating system version = ", "os.version", printWriter);
            printSystemProperty(security, "File separator (\"/\" on UNIX) = ", "file.separator", printWriter);
            printSystemProperty(security, "Path separator (\":\" on UNIX) = ", "path.separator", printWriter);
            printSystemProperty(security, "User's account name = ", "user.name", printWriter);
            printSystemProperty(security, "User's home directory = ", "user.home", printWriter);
            printSystemProperty(security, "User's current working directory = ", "user.dir", printWriter);
            printWriter.println(header + "END TRACE_DRIVER_CONFIGURATION");
            printWriter.flush();
        }
    }

    private static void printSystemProperty(SecurityManager security,
                                            String prefix,
                                            String property,
                                            PrintWriter printWriter) {
        String header = "[derby] ";
        synchronized (printWriter) {
            try {
                if (security != null) {
                    security.checkPropertyAccess(property);
                }
                String result = System.getProperty(property);
                printWriter.println(header + prefix + result);
                printWriter.flush();
            } catch (SecurityException e) {
                printWriter.println(header + 
                    msgutil.getTextMessage(MessageId.SECURITY_MANAGER_NO_ACCESS_ID, property));
                printWriter.flush();
            }
        }
    }

    // printWriter synchronized by caller
    private static void detectLocalHost(
            SecurityManager security,
            PrintWriter printWriter) {

        String header = "[derby] ";
        // getLocalHost() will hang the HotJava 1.0 browser with a high security manager.
        if (security == null) {
            try {
                printWriter.print(header + "Detected local client host: ");
                printWriter.println(InetAddress.getLocalHost().toString());
                printWriter.flush();
            } catch (UnknownHostException e) {
                printWriter.println(header + 
                    msgutil.getTextMessage(MessageId.UNKNOWN_HOST_ID, e.getMessage()));
                printWriter.flush();
            }
        }
    }
}
