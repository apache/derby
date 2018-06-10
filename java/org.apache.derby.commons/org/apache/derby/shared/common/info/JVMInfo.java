/*

   Derby - Class org.apache.derby.shared.common.info.JVMInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.shared.common.info;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

// As an exception to the rule we import SanityManager from the shared package
// here, because the JVMInfo class is included in both derby.jar and
// derbyclient.jar. Pulling in the class from the shared package allows us to
// unseal the shared package only (leaving iapi.services.sanity sealed).
import org.apache.derby.shared.common.sanity.SanityManager;

/**
	This class is used to determine which Java specification Derby will run at.
    For a useful discussion of how this class is used, please see DERBY-3176.
 */
public abstract class JVMInfo
{
	/**
		The JVM's runtime environment.
		<UL>
		<LI> 1 - not used was JDK 1.1
		<LI> 2 - not used, was for JDK 1.2 and 1.3
        <LI> 4 - not used, was for JDK 1.4.0 or 1.4.1
        <LI> 5 - not used, was for JDK 1.4.2
        <LI> 6 - not used, was for JDK 1.5
        <LI> 7 - not used, was for JDK 1.6
        <LI> 8 - not used, was for JDK 1.7
        <LI> 9 - J2SE_18 - JDK 1.8
		</UL>
	*/
	public static final int JDK_ID;

    public static final int J2SE_18 = 9;   // Java SE 8

    public static int jdbcMajorVersion()
    { 
        return 4; 
    }

    public static int jdbcMinorVersion()
    {
        switch (JDK_ID)
        { 
        case J2SE_18: 
        default: return 2; 
        }
    } 

	static 
	{
		int id;

		//
		// If the property java.specification.version is set, then try to parse
		// that.  Anything we don't recognize, default to Java 2 platform
		// because java.specification.version is a property that is introduced
		// in Java 2.  We hope that JVM vendors don't implement Java 1 and
		// set a Java 2 system property.
		// 
		// Otherwise, see if we recognize what is set in java.version.
		// If we don't recognize that, or if the property is not set, assume
        // version 1.8, which is the lowest level we support.
		//
        String javaVersion = "1.8";
		try {
            javaVersion =
                System.getProperty("java.specification.version", javaVersion);
		} catch (SecurityException se) {
			// some vms do not know about this property so they
			// throw a security exception when access is restricted.
		}

        if (javaVersion.equals("1.8")) {
            id = J2SE_18;
        }
        else
        {
            // Assume our lowest support unless the java spec
            // is greater than our highest level.
            id = J2SE_18;

            try {

                // Extract major and minor version out of the spec version.
                String[] ver = javaVersion.split("[.]");
                int major = ver.length >= 1 ? Integer.parseInt(ver[0]) : 0;
                int minor = ver.length >= 2 ? Integer.parseInt(ver[1]) : 0;

                // The highest level currently supported is 1.8. Use that
                // level if the spec version is greater than or equal to it.
                final int highestMajor = 1;
                final int highestMinor = 8;
                if (major > highestMajor ||
                        (major == highestMajor && minor >= highestMinor)) {
                    id = J2SE_18;
                }

            } catch (NumberFormatException nfe) {
            }
        }

		JDK_ID = id;
	}

	/**
		Return Derby's understanding of the virtual machine's environment.

        @return a string description of the virtual machine's version
	*/
	public static String derbyVMLevel()
	{
		String jdbcVersion = jdbcMajorVersion() + "." + jdbcMinorVersion();
		switch (JDK_ID)
		{
        case J2SE_18: return "Java SE 8 - JDBC " + jdbcVersion;
		default: return "?-?";
		}
	}

    /**
     * Get system property.
     *
     * @param name name of the property
     */
    private static String getSystemProperty(final String name) {
        
        return AccessController
                .doPrivileged(new java.security.PrivilegedAction<String>() {
                    
                    public String run() {
                        return System.getProperty(name);
                        
                    }
                    
                });
    }
    
    /**
     * Check whether this is sun jvm.
     *
     * @return true if it is sun jvm, false if it is not sun jvm
     */
    public static final boolean isSunJVM() {
        String vendor = getSystemProperty("java.vendor");
        return "Sun Microsystems Inc.".equals(vendor)
                || "Oracle Corporation".equals(vendor);
    }
    
    /**
     * Check whether this is IBM jvm.
     *
     * @return true if it is IBM jvm, false if it is not IBM jvm
     */
    public static final boolean isIBMJVM() {
        return ("IBM Corporation".equals(getSystemProperty("java.vendor")));
    }
    
    /**
     * For IBM jvm, this method will dump more diagnostic information to file.
     * JVM specific code for other vender can be added. DERBY-4856 
     *  
     */
    public static void javaDump() {
        if (isIBMJVM()) {
            Class<?> ibmc = null;
            try {
                ibmc = Class.forName("com.ibm.jvm.Dump");
                final Method ibmm = ibmc.getMethod("JavaDump", new Class<?>[] {});
                
                AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                    public Object run() throws IllegalAccessException,
                            MalformedURLException, InstantiationException,
                            InvocationTargetException {
                        return ibmm.invoke(null, new Object[] {});
                    }
                });
            } catch (Exception e) {
                if (SanityManager.DEBUG) {
                    SanityManager
                            .THROWASSERT(
                                    "Failed to execute com.ibm.jvm.Dump.JavaDump in IBM JVM",
                                    e);
                }
            }
        }
    }

    /**
     * Determine whether we are running in a constrained environment.
     * @return true if JNDI is available
     */
    public static boolean hasJNDI() {
        try {
            Class.forName("javax.naming.Referenceable");
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }
}
