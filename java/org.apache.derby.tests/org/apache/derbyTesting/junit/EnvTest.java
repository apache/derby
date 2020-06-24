/*
 *
 * Derby - Class org.apache.derbyTesting.junit.EnvTest
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

import java.io.PrintStream;
import java.security.AccessController;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

/**
 * Simple Junit "test" that runs a number of fixtures to
 * show the environment a test would run in.
 * A fixture changes its name based upon the return
 * of a method that checks for some environmental condition,
 * e.g. does this vm support JDBC 3.
 * Meant as a simple aid to help determine any environment problems.
 *
 */
public class EnvTest extends TestCase {
	
	public EnvTest(String name)
	{
		super(name);
	}
	
    /**
     * Print trace string.
     * @param text String to print
     */
    public void traceit(final String text) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4808
        setName(text);
        PrintStream out = System.out;
        String KEY_TRACE = "derby.tests.trace";
        if (Boolean.valueOf(
                getSystemProperties().getProperty(KEY_TRACE)).
                booleanValue()) 
        {
            out.println(text);
        }
    }
    
    /**
     * Get the system properties in a privileged block.
     *
     * @return the system properties.
     */
    public  static final Properties getSystemProperties() {
        // Fetch system properties in a privileged block.
        return AccessController.doPrivileged(
                new PrivilegedAction<Properties>() {
            public Properties run() {
                return System.getProperties();
            }
        });
    }
	/*
	** Tests of the JDBC.vmSupportsXXX to see which JDBC support is available.
	*/
	public void testJSR169() {
        traceit(String.valueOf(JDBC.vmSupportsJSR169()) +
            "_vmSupportsJSR169()");
	}
	public void testJDBC3() {
        traceit(String.valueOf(JDBC.vmSupportsJDBC3()) +
                "_vmSupportsJDBC3()");
	}
    public void testJDBC4() {
        traceit(String.valueOf(JDBC.vmSupportsJDBC4()) +
                "_vmSupportsJDBC4()");
    }
    public void testJDBC41() {
        traceit(String.valueOf(JDBC.vmSupportsJDBC41()) +
                "_vmSupportsJDBC41()");
    }
    public void testJDBC42() {
        traceit(String.valueOf(JDBC.vmSupportsJDBC42()) +
                "_vmSupportsJDBC42()");
    }
	
	
	/*
	** Tests of the Derby.hasXXX to see which Derby code is
	** available for the tests.
	*/
	public void testHasServer() {
        traceit(String.valueOf(Derby.hasServer() + "_hasServer"));
	}
	public void testHasClient() {
        traceit(String.valueOf(Derby.hasClient() + "_hasClient"));
	}
	public void testHasEmbedded() {
        traceit(String.valueOf(Derby.hasEmbedded() + "_hasEmbedded"));
	}
	public void testHasTools() {
        traceit(String.valueOf(Derby.hasTools() + "_hasTools"));
	}
    /*
    ** XML related tests
    */
    public void testClasspathHasXalanAndJAXP() {
        traceit(String.valueOf(XML.classpathHasJAXP() + "_classpathHasJAXP"));
    }
    public void testClasspathMeetsXMLReqs() {
        traceit(String.valueOf(XML.classpathMeetsXMLReqs() +
                "_classpathMeetsXMLReqs"));
    }
    public void testHasLuceneCoreJar() {
        traceit(String.valueOf(JDBC.HAVE_LUCENE_CORE + "_hasLuceneCore"));
    }
    public void testHasLuceneQueryParserJar() {
        traceit(String.valueOf(JDBC.HAVE_LUCENE_QUERYPARSER + 
                "_hasLuceneQueryParser"));
    }
    public void testHasLuceneAnalyzersJar() {
        traceit(String.valueOf(JDBC.HAVE_LUCENE_ANALYZERS + 
                "_hasLuceneAnalyzers"));
    }
    
    public void testHasLDAPConfig() {
        // we need a bunch of properties for ldap testing
        Properties props = getSystemProperties();
        List<String> ldapSettings = new ArrayList<String>();
        ldapSettings.add(props.getProperty("derbyTesting.ldapPassword"));
        ldapSettings.add(props.getProperty("derbyTesting.ldapServer"));
        ldapSettings.add(props.getProperty("derbyTesting.ldapPort"));
        ldapSettings.add(props.getProperty("derbyTesting.dnString"));
        ldapSettings.add(props.getProperty("derbyTesting.ldapContextFactory"));
        boolean sofarsogood=true;
        for (String s:ldapSettings) {
            if (s == null || s.length()== 0 || s.isEmpty())
            {
                sofarsogood=false;
                break;
            }
        }
        traceit(String.valueOf(sofarsogood) + "_hasLDAPConfiguration");
    }
    
    public void testHasJNDISupport() {
        traceit(String.valueOf(JDBC.vmSupportsJNDI() + 
                "_classpathMeetsJNDIReqs"));
    }
    
    public void testHasBasicEncryptionSupport() {
        try {
            // First check for the preferred default, and return it if present
            MessageDigest.getInstance("SHA-256");
            traceit("true_hasBasicEncryptionAlgorithmSupport");
        } catch (NoSuchAlgorithmException nsae) {
            // Couldn't find the preferred algorithm
            traceit("false_hasBasicEncryptionAlgorithmSupport");
        }
    }
    
    public void testHasSubStandardEncryptionSupport() {
        try {
            // First check for the preferred default, and return it if present
            MessageDigest.getInstance("SHA-1");
            traceit("true_hasSubstandardEncryptionAlgorithmSupport");
        } catch (NoSuchAlgorithmException nsae) {
            traceit("false_hasSubStandardEncryptionAlgorithmSupport");
        }
    }
}
