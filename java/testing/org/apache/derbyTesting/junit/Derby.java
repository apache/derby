/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.Derby
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import junit.framework.Assert;

/**
 * Derby related utility methods for the JUnit tests.
 * The class assumes the tests are either being run
 * from the build classes folder or from the standard
 * jar files (or a subset of the standard jars).
 * <BR>
 * If the tests are being run from the classes then
 * it is assumed all the functionality is available,
 * otherwise the functionality will be driven from which
 * jar files are on the classpath. E.g. if only
 * derby.jar is on the classpath then the hasXXX() methods
 * will return false except hasEmbedded().
 */
public class Derby {
    
    /**
     * Returns true if the embedded engine is available to the tests.
     */
    public static boolean hasEmbedded()
    {
        // classes folder - assume all is available.
        if (!SecurityManagerSetup.isJars)
            return true;

        return hasCorrectJar("/derby.jar",
               "org.apache.derby.authentication.UserAuthenticator");
    }
    /**
     * Returns true if the network server is available to the tests.
     */
    public static boolean hasServer()
    {
        // classes folder - assume all is available.
        if (!SecurityManagerSetup.isJars)
            return true;
        
        return hasCorrectJar("/derbynet.jar",
                             "org.apache.derby.drda.NetworkServerControl");
    }
    /**
     * Returns true if the tools are available to the tests.
     */
    public static boolean hasTools()
    {
        // classes folder - assume all is available.
        if (!SecurityManagerSetup.isJars)
            return true;
            
        return hasCorrectJar("/derbytools.jar",
                "org.apache.derby.tools.ij");
    }
    /**
     * Returns true if the derby client is available to the tests.
     */
    public static boolean hasClient()
    {
        // classes folder - assume all is available.
        if (!SecurityManagerSetup.isJars)
            return true;
        
        return hasCorrectJar("/derbyclient.jar",
                "org.apache.derby.jdbc.ClientDataSource");
    }
    
    private static boolean hasCorrectJar(String jarName, String className)
    {
        URL url = SecurityManagerSetup.getURL(className);
        if (url == null)
            return false;
        
        return url.toExternalForm().endsWith(jarName);
    }
}
