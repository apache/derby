/* 

   Derby - Class org.apache.derbyTesting.functionTests.harness.RunList

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.harness;

//import org.apache.derby.tools.sysinfo;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.PrintStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.ClassNotFoundException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.StringTokenizer;

public class RunList
{

	static String jvmName = "currentjvm";
	static String javaCmd = "java";
	static String javaArgs;
	static jvm jvm;
	static String javaVersion; // System.getProperty("java.version")
	static String majorVersion;
	static String minorVersion;
	static String jversion; // to pass jvm to RunTest as -Djvm=1.2 etc.
	static int iminor;
	static int imajor;
	static boolean skip = false;
	static boolean driverNotFound = false;
	static boolean needSync = false;
	static boolean needJdk12 = false;
	static boolean needJdk12ext = false;
	static boolean excludedFromJCC = false;
	static Boolean needIBMjvm = null;
	static boolean needJdk14 = false;
        static boolean needEncryption = false;
	static String jvmflags;
	static String testJavaFlags;
	static String classpath;
	static String classpathServer;
    static String framework;
    static String usesystem;
    static String upgradetest;
    static String jarfile;
    static String useoutput;
	static String keepfiles = "false";
	static String encryption;
	static String testEncryptionProvider;
	static String jdk12test;
	static String jdk12exttest;
	static String jdk14test;
	static String runwithibmjvm = null;
	static String runwithj9;
	static String runwithjvm;
	static String excludeJCC;
	static boolean useprocess = true;
	static String skipsed = "false";
	static boolean fw_set = false;
	static String systemdiff = "false";
	static String suiteName = "";
	static String fullsuiteName = "";
	static String topSuiteName = ""; // The very top suite creating RunLists
	static String topParentSuite = ""; // The "subparent" of the very top suite
	static String topSpecialProps = ""; // special properties at the top suite
	static String otherSpecialProps = ""; // special properties (individual suite)
	static String ijdefaultResourcePackage; // for ij tests, the package resource
	static String outcopy; // cases where copyfiles should go to outDir
	static String userdir; // current user directory
	static String mtestdir; // required by multi tests
	static boolean verbose = false; // for debug output
	static String reportstderr;
	static String timeout;
	static String shutdownurl;
    static PrintWriter pwOut; // for writing suite output
	static String outputdir; // location of output (default is userdir)
	static String topsuitedir; // for nested suites, need top output location
	static String topreportdir; // where to place the .pass and .fail files
	static String canondir; // location of masters (default is master)
	static String bootcp; // for j9 bootclasspath
	static String serverJvm; // for starting another jvm for networkserver, j9 only for now.
    static File outDir; // test out dir
    static File outFile; // suite output file
    static File runDir; // location of suite.runall (list of tests)
	static File runFile; // suite.runall file
	static Properties suiteProperties;
	static Properties specialProperties; // for testSpecialProps
	static BufferedReader runlistFile;

    public RunList()
    {
    }

    /**
    * RunList
    * suitesToRun: a Vector of suites from RunSuite
    * outDir: The output directory for the suite(s)
    * pwOut: The output for writing suite and test results
    * suiteProperties: From RunSuite for the top suite
    * (individual suites in the vector may have their own
    * properties which must also be located and applied)
    */

    public RunList(Vector suitesToRun, 
        File runDir, File outDir, PrintWriter pwOut,
        Properties suiteProperties, 
        Properties specialProperties, 
        String topParentSuite)
        throws ClassNotFoundException, IOException, Exception
    {
        this.runDir = runDir;
        this.outDir = outDir;
        this.pwOut = pwOut;
        this.suiteProperties = suiteProperties; // usual suite props
        this.specialProperties = specialProperties; // for special test Flags
        this.topSuiteName = suiteProperties.getProperty("suitename");
        //System.out.println("----------------------------------------");
        //System.out.println("RunList topSuiteName= " + topSuiteName);
        this.topParentSuite = topParentSuite;
        //System.out.println("topParentSuite= " + topParentSuite);
        
        // Run the suites
        runSuites(suitesToRun);
    }

    private static void runSuites(Vector suitesToRun)
        throws ClassNotFoundException,
        FileNotFoundException, IOException, Exception
    {
        // For each suite, locate its properties and runall files
        // which should be in the "suites" dir or user.dir
        String suiteName = "";
        userdir = System.getProperty("user.dir");
        //System.out.println("Number of suites in list = " + suitesToRun.size());
        Properties p = null;
        
        // First get the top level suiteProperties since some
        // special properties might need to be used by all sub-suites
        setTopSuiteProperties();
        // Now set the properties for the topParentSuite (a sub-parent of the top)
        Properties topParentSuiteProps = 
            locateSuiteProperties(topParentSuite, suiteProperties, true, true);
        setSuiteProperties(topParentSuiteProps, topParentSuite, suiteProperties, true, true);
            
        // Now handle the list of child suites under this parent
        for (int i = 0; i < suitesToRun.size(); i++)
        {
            /* Note: nesting of suites can be complex, especially if the
             subsuites of the top suite also contain subsuites; we must take
             care in setting of special properties like framework which may
             need to propagate to its subsuites, but not back to the very top
            */
            Properties subProps = null;
            fullsuiteName = (String)suitesToRun.elementAt(i);
            //System.out.println("fullsuiteName: " + fullsuiteName);
            String subSuite = fullsuiteName.substring(0,fullsuiteName.lastIndexOf(":"));
            //System.out.println("subSuite: " + subSuite);
            if ( !subSuite.equals(topParentSuite) )
            {
                subProps = locateSuiteProperties(subSuite, topParentSuiteProps, true, false);
            }
            else
            {
                // reset in case a previous subsuite had set framework, etc
                subProps = topParentSuiteProps;
            }
            setSuiteProperties(subProps, subSuite, topParentSuiteProps, true, false);
            
            // Now handle the child suite of this subSuite
            suiteName = fullsuiteName.substring(fullsuiteName.lastIndexOf(":")+1);
            //System.out.println("child suiteName: " + suiteName);
                
            p = locateSuiteProperties(suiteName, subProps, false, false);
            setSuiteProperties(p, suiteName, subProps, false, false);

            // Now locate the suite runall file containing the tests
            String runfile = "suites" + '/' + suiteName + ".runall";

            InputStream is =  org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(runfile);
            if (is == null)
            {
                // Look in userdir
                is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(userdir + '/' + suiteName + ".runall");
            }
            if (is == null)
            {
                System.out.println("Suite runall file not found for " + suiteName);
                continue;
            }

            // Create a BufferedReader to read the list of tests to run
            runlistFile = new BufferedReader(new InputStreamReader(is));
            if (runlistFile == null)
            {
                System.out.println("The suite runall file could not be read.");
            }
            else
            {
                String startTime = CurrentTime.getTime();
                pwOut.println("**** Start SubSuite: " + fullsuiteName +
                    " jdk" + javaVersion +
                    " " + startTime + " ****");
                if ( (framework != null) && (framework.length()>0) )
                {
                    pwOut.println("Framework: " + framework);
                }
                else
                    pwOut.println("Framework: No special framework.");
                
                // Create the file to list the suites that get skipped
	            File f = new File(outDir, topSuiteName);
	            File skipFile = new File(f, topSuiteName+".skip");

		    //we catch an IOException here to work around a jvm bug on the Psion.
		    PrintStream ps = null;
		    try { ps = new PrintStream
		            ( new FileOutputStream(skipFile.getCanonicalPath(),true) ); }
		    catch (IOException e) {
			FileWriter fw = new FileWriter(skipFile);
			fw.close();
			ps = new PrintStream
			    ( new FileOutputStream(skipFile.getCanonicalPath(),true) );
		    }

                System.out.println("Now run the suite's tests");
                //System.out.println("shutdownurl: " + shutdownurl);
                
                if (skip) // Skip a suite under certain environments
				{
				    addToSkipFile(topSuiteName+":"+fullsuiteName, ps);
					if(driverNotFound)
                    	pwOut.println("Cannot run the suite, framework driver not found");
					else if(needSync)
                    	pwOut.println("Cannot run the suite, sync product not found");
					else if(needJdk12ext)
                    	pwOut.println("Cannot run the suite, requires jdk12 or higher with extensions");
					else if(needJdk12)
                    	pwOut.println("Cannot run the suite, requires jdk12 or higher, have jdk" + javaVersion);
					else if(needJdk14)
                    	pwOut.println("Cannot run the suite, requires jdk14 or higher, have jdk" + javaVersion);
					else if(needJdk14)
                    	pwOut.println("Cannot run the suite, requires jdk14 or higher, have jdk" + javaVersion);
					else if(excludedFromJCC)
                    	pwOut.println("Cannot run the suite on JCC version " + excludeJCC + " or lower.");                                     
                                        else if((needIBMjvm == null || needIBMjvm.booleanValue() == false))
                    	pwOut.println("Cannot run the suite, requires IBM jvm, jvm vendor is " + System.getProperty("java.vendor"));
					else
                    	pwOut.println("Cannot run the suite, have jdk" + javaVersion);
				 }
                else
                {
                    System.out.println("Run the tests...");
                    // Unjar any jarfile define for an upgrade suite
                    //System.out.println("jarfile: " + jarfile);
                    if (jarfile != null)
                    {
                        //System.out.println("unjar jar file...");
                        UnJar uj = new UnJar();
                        uj.unjar(jarfile, outDir.getCanonicalPath(), true);
                        if ( (upgradetest.equals("true")) && (suiteName.startsWith("convert")) )
                        {
                            // need to rename the directory
                            // such as kimono -- rename to convertKimono
			    String tmpname = jarfile.substring(0, jarfile.indexOf("JAR"));
                            File tmp = new File(outDir, tmpname);
			    File convert = new File(outDir, usesystem);
                            boolean renamed = tmp.renameTo(convert);
                            //System.out.println("renamed: " + renamed);
                        }
                    }
                    
                    // Run the tests for this suite
                    runTests(p, fullsuiteName);
                }
                
                String endTime = CurrentTime.getTime();
                pwOut.println("**** End SubSuite: " + fullsuiteName +
                    " jdk" + javaVersion +
                    " " + endTime + " ****");
                //System.out.println("--------------------------------------");
                ps.close();
            }
        }
    }


    private static void runTests(Properties suiteProps, String suite)
        throws IOException, Exception
    {
	    // save a copy of the system properties at this point; when runing with
	    // java threads we need to reset the system properties to this list;
	    // otherwise we start to accumulate extraneous properties from
	    // individual tests (does not happen with exec (useprocess==true)
	    // because each test case has its own fresh VM
	    ManageSysProps.saveSysProps();

        // Build command string for RunTest()
        StringBuffer sb = new StringBuffer();
	    jvm = jvm.getJvm(jvmName);
	    Vector jvmProps = new Vector();
	    if ((javaCmd.length()>0) )
	    {
	        jvm.setJavaCmd(javaCmd);
	        jvmProps.addElement("javaCmd=" + javaCmd);
	    }
        if ( (testJavaFlags != null) && (testJavaFlags.length()>0) )
            jvmProps.addElement("testJavaFlags=" + testJavaFlags);
	    if (classpath != null)
	        jvmProps.addElement("classpath=" + classpath);
	    if (classpathServer != null)
	        jvmProps.addElement("classpathServer=" + classpathServer);
	    if (jversion != null)
	        jvmProps.addElement("jvm=" + jversion);
        if (framework != null)
            jvmProps.addElement("framework=" + framework);
        if (usesystem != null)
            jvmProps.addElement("usesystem=" + usesystem);
        if (shutdownurl != null)
            jvmProps.addElement("shutdownurl=" + shutdownurl);
        if (upgradetest != null)
            jvmProps.addElement("upgradetest=" + upgradetest);       
        if (outcopy != null)
            jvmProps.addElement("outcopy=" + outcopy);
        if (useoutput != null)
            jvmProps.addElement("useoutput=" + useoutput);
        if (verbose == true)
            jvmProps.addElement("verbose=true");
        if ( (reportstderr != null) && (reportstderr.length()>0) )
            jvmProps.addElement("reportstderr=" + reportstderr);
        
        if ( (jvmflags != null) && (jvmflags.length()>0) )
        {
            // We want to pass this down to RunTest so it will
            // run an individual test with jvmflags like -nojit
            jvmProps.addElement("jvmflags=" + '"' + jvmflags + '"');
        }
        
        if ( (timeout != null) && (timeout.length()>0) )
        {
            if (useprocess)
			{
                jvmProps.addElement("timeout=" + timeout);            
            }
            else
            {
                org.apache.derbyTesting.functionTests.harness.RunTest.timeoutStr = timeout;
            }
        }
		if (Boolean.getBoolean("listOnly"))
			jvmProps.addElement("listOnly=true");

        if (encryption != null)
            jvmProps.addElement("encryption=" + encryption);
        if (testEncryptionProvider != null)
            jvmProps.addElement("testEncryptionProvider=" + testEncryptionProvider);
        if (jdk12test != null)
            jvmProps.addElement("jdk12test=" + jdk12test);
        if (jdk12exttest != null)
            jvmProps.addElement("jdk12exttest=" + jdk12exttest);
        if (jdk14test != null)
            jvmProps.addElement("jdk14test=" + jdk14test);
        if (keepfiles != null)
            jvmProps.addElement("keepfiles=" + keepfiles);
        if ( (outputdir != null) && (outputdir.length()>0) )
        {
            jvmProps.addElement("outputdir=" + outputdir);
        }
        if ( (topsuitedir != null) && (topsuitedir.length()>0) )
            jvmProps.addElement("topsuitedir=" + topsuitedir);
        else
            jvmProps.addElement("topsuitedir=" + outputdir);
        if (topreportdir != null)
            jvmProps.addElement("topreportdir=" + topreportdir);
        else
            jvmProps.addElement("topreprtdir=" + topsuitedir);
        if ( (runDir != null) && (runDir.exists()) )
            jvmProps.addElement("rundir=" + runDir.getCanonicalPath());
        if ( (bootcp != null) && (bootcp.length()>0) )
            jvmProps.addElement("bootcp=" + bootcp);
        if ( (serverJvm != null) && (serverJvm.length()>0) )
            jvmProps.addElement("serverJvm=" + serverJvm);
        if ( useprocess == false )
            jvmProps.addElement("useprocess=false");
        if ( skipsed.equals("true") )
            jvmProps.addElement("skipsed=true");
        if ( systemdiff != null )
            jvmProps.addElement("systemdiff=" + systemdiff);
        if ( ijdefaultResourcePackage != null )
            jvmProps.addElement("ij.defaultResourcePackage=" + ijdefaultResourcePackage);
        if ( mtestdir != null )
            jvmProps.addElement("mtestdir=" + mtestdir);
        if (topSpecialProps.length()>0)
        {
            jvmProps.addElement("testSpecialProps=" + topSpecialProps +
            ((otherSpecialProps.length()>0)?
             ("^" + otherSpecialProps)
             :"")
            );
        }
        else if (otherSpecialProps.length()>0)
            jvmProps.addElement("testSpecialProps=" + otherSpecialProps);

        jvmProps.addElement("suitename=" + suite);
        
        if ( (topSuiteName != null) && (topSuiteName.length()>0) )
            jvmProps.addElement("topsuitename=" + topSuiteName);

        if (classpath != null)
            jvm.setClasspath(classpath);

        jvm.setD(jvmProps);
        Vector v = jvm.getCommandLine();
        v.addElement("org.apache.derbyTesting.functionTests.harness.RunTest");

        String str = "";
	    String lastTest = null;
	    String skipTo = System.getProperties().getProperty("skipToFile");
	    String stopAfter = System.getProperties().getProperty("stopAfterFile");
        // Read the individual tests
        // Example: "lang/avg.sql" or "conn/resultset.java"
        while ( (str = runlistFile.readLine()) != null )
        {
	        // skip tests if specified
	        if (skipTo != null && !str.equals(skipTo)) 
	            continue;
	        else 
		        skipTo = null;
	        if (stopAfter != null && lastTest != null && lastTest.equals(stopAfter)) break;
            // Create the command for RunTest
            // Create a string array from the vector
            String testCmd[] = new String[v.size() + 1];
            StringBuffer verboseSb = new StringBuffer();
            int i = 0;
            for (i = 0; i < v.size(); i++)
            {
                testCmd[i] = (String)v.elementAt(i);
                verboseSb.append(testCmd[i] + " ");
            }
            testCmd[i++] = str;
            verboseSb.append(str + " ");
            //if (verbose) 
                //System.out.println("Execute command: " + verboseSb.toString());
            
	        String uc = System.getProperties().getProperty("useCommonDB");
		    if (uc == null) uc = "false";
            if ( useprocess == true && uc.equals("true")==false)
            {
                System.out.println("Execute command: " + verboseSb.toString());
                
                // Now execute the command to run the test
        		Process pr = null;
        		try
        		{
                    pr = Runtime.getRuntime().exec(testCmd);

                    // We need the process inputstream to capture into the output file
                    BackgroundStreamDrainer stdout =
                        new BackgroundStreamDrainer(pr.getInputStream(), null);
                    BackgroundStreamDrainer stderr =
                        new BackgroundStreamDrainer(pr.getErrorStream(), null);

                    pr.waitFor();

                    String result = HandleResult.handleResult(pr.exitValue(),
                        stdout.getData(), stderr.getData(), pwOut);
                    pr.destroy();
                }
                catch(Throwable t)
                {
                    System.out.println("Process exception: " + t.getMessage());
                    if (pr != null)
                    {
                        pr.destroy();
                        pr = null;
                    }
                }
            }
            else
            {
                String[] args = new String[6];
                args[0] = str; // the test name
                if ( ijdefaultResourcePackage != null )
                    args[1] = ijdefaultResourcePackage;
                else
                    args[1] = "/org/apache/derbyTesting/functionTests/";
                if ( usesystem != null )
                    args[2] = usesystem;
                else
                    args[2] = "";
                args[3] = "noprocess";
                if ( shutdownurl != null)
                    args[4] = shutdownurl;
                else
                    args[4] = "";
                args[5] = suite;
                org.apache.derbyTesting.functionTests.harness.RunTest.main(args);
                // Write any diff to the suite's output
                String tmp = str.substring(str.indexOf("/") + 1, str.lastIndexOf("."));
                String diffname = tmp + "." + "diff";
                File diffFile = new File(outDir, diffname);
                if ( (diffFile != null) && (diffFile.exists()) )
                {
                    BufferedReader inFile =
                        new BufferedReader(new FileReader(diffFile));
                    String diffLine = "";
                    while ( (diffLine = inFile.readLine()) != null )
                    {
                        pwOut.println(diffLine);
                    }
                }

            }
	    // reset the system properties to prevent confusion
	    // when running with java threads
	    ManageSysProps.resetSysProps();
	    lastTest = str;
        }
    }
    
    /**
    * Locate the suite's properties file
    */
    public static Properties locateSuiteProperties(String suiteName, 
        Properties parentProps, boolean isParent, boolean isTop)
        throws ClassNotFoundException, IOException, Exception
    {
        // Check for suite properties
        //System.out.println("Checking for suite properties");
        String suitePropsName = "suites" + '/' + suiteName + ".properties";

        InputStream is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(suitePropsName);
        if (is == null)
        {
            // Look in userdir
            suitePropsName = userdir + '/' + suiteName + ".properties";
            is = org.apache.derbyTesting.functionTests.harness.RunTest.loadTestResource(suitePropsName);
        }
        Properties p = new Properties();
        // Reset these properties
        if (isParent)
        {
            usesystem = null;
            upgradetest = null;
            jarfile = null;
            outcopy = null;
            useoutput = null;
            mtestdir = null;
            skipsed = "false";
            //outputdir = outDir.getCanonicalPath();
        }
        if (is != null)
        {
            p.load(is);
            is = null;
        }
        else
        {
            // Reset framework to the parent suite's framework, if any
            // because framework may have been set by previous suite
            framework = parentProps.getProperty("framework");
            serverJvm = parentProps.getProperty("serverJvm");
            // Do the same for ij.defaultResourcePackage
            ijdefaultResourcePackage =
                parentProps.getProperty("ij.defaultResourcePackage");
            // And do the same for encryption
            encryption = parentProps.getProperty("encryption");
            testEncryptionProvider = parentProps.getProperty("testEncryptionProvider");
            // And do the same for jdk12test
            jdk12test = parentProps.getProperty("jdk12test");
            jdk12exttest = parentProps.getProperty("jdk12exttest");
	    // and jdk14test
            jdk14test = parentProps.getProperty("jdk14test");
            runwithj9 = parentProps.getProperty("runwithj9");
            runwithibmjvm = parentProps.getProperty("runwithibmjvm");
            String testJVM = (jvmName.startsWith("j9") ? "j9" : jvmName);
            runwithjvm = parentProps.getProperty("runwith" + testJVM);
            excludeJCC = parentProps.getProperty("excludeJCC");
        }                
        return p;
    }
                

    /**
    * Properties which may be defined for all suites
    * at the top level suite (such as "nightly")
    */
    private static void setTopSuiteProperties()
        throws ClassNotFoundException, IOException
    {
		framework = suiteProperties.getProperty("framework");
		
		jversion = suiteProperties.getProperty("jversion");
		//System.out.println("RunList top jversion= " + jversion);
		
		jvmName = suiteProperties.getProperty("jvm");
		String j9config = System.getProperty("com.ibm.oti.configuration");	
		if (j9config != null)
			if (j9config.equals("foun")) 
				jvmName="j9_foundation";
			else if (j9config.equals("max"))
				jvmName="j9_13";
		if (jversion == null)
		    javaVersion = System.getProperty("java.version");
		else
		    javaVersion = jversion;
		    
		//System.out.println("RunList setTopSuiteProperties javaVersion: " + javaVersion);

		javaCmd = suiteProperties.getProperty("javaCmd");
		if (javaCmd == null)
		    javaCmd = "java";
		else if (javaCmd.equals("jview"))
		    jvmName = "jview";
        
        JavaVersionHolder jvh = new JavaVersionHolder(javaVersion);
        majorVersion = jvh.getMajorVersion();
        minorVersion = jvh.getMinorVersion();
        iminor = jvh.getMinorNumber();
        imajor = jvh.getMajorNumber();
        
		if ( (jvmName == null) || (jvmName.equals("jview")) )
		{
		    if ( (iminor < 2) && (imajor < 2) )
		        jvmName = "currentjvm";
		    else
		        jvmName = "jdk" + majorVersion + minorVersion;
		}
		
		jvmflags = suiteProperties.getProperty("jvmflags");
		testJavaFlags = suiteProperties.getProperty("testJavaFlags");
		classpath = suiteProperties.getProperty("classpath");
		classpathServer = suiteProperties.getProperty("classpathServer");
		usesystem = suiteProperties.getProperty("usesystem");
		upgradetest = suiteProperties.getProperty("upgradetest");
        outcopy = suiteProperties.getProperty("outcopy");
		useoutput = suiteProperties.getProperty("useoutput");
		encryption = suiteProperties.getProperty("encryption");
		testEncryptionProvider = suiteProperties.getProperty("testEncryptionProvider");
		jdk12test = suiteProperties.getProperty("jdk12test");
		jdk12exttest = suiteProperties.getProperty("jdk12exttest");
		jdk14test = suiteProperties.getProperty("jdk14test");
		runwithibmjvm = suiteProperties.getProperty("runwithibmjvm");
		runwithj9 = suiteProperties.getProperty("runwithj9");
		String testJVM = (jvmName.startsWith("j9") ? "j9" : jvmName);
		runwithjvm = suiteProperties.getProperty("runwith" + testJVM);
		excludeJCC = suiteProperties.getProperty("excludeJCC");
		keepfiles = suiteProperties.getProperty("keepfiles");
		systemdiff = suiteProperties.getProperty("systemdiff");
		outputdir = suiteProperties.getProperty("outputdir");
		if (outputdir == null)
		    outputdir = userdir;
		topsuitedir = suiteProperties.getProperty("topsuitedir");
		if (topsuitedir == null)
		    topsuitedir = outputdir;
		bootcp = suiteProperties.getProperty("bootcp");
		serverJvm = suiteProperties.getProperty("serverJvm");
		canondir = suiteProperties.getProperty("canondir");
		mtestdir = suiteProperties.getProperty("mtestdir");
		String usepr = suiteProperties.getProperty("useprocess");
		if ( (usepr != null) && (usepr.equals("false")) )
		    useprocess = false;
		skipsed = suiteProperties.getProperty("skipsed");
		String dbug = suiteProperties.getProperty("verbose");
		if ( (dbug != null) && (dbug.equals("true")) )
		    verbose = true;
		reportstderr = suiteProperties.getProperty("reportstderr");
		timeout = suiteProperties.getProperty("timeout");
		shutdownurl = suiteProperties.getProperty("shutdownurl");
		topSuiteName = suiteProperties.getProperty("suitename");
		ijdefaultResourcePackage =
		    suiteProperties.getProperty("ij.defaultResourcePackage");
        // The top level suiteProperties may have special
        // properties which need to be added to testSpecialProps
        if ( (specialProperties != null) && (!specialProperties.isEmpty()) )
        {
            //System.out.println("Top suite has special props");
            setSpecialProps(specialProperties, true);
        }
    }

    /**
    * Properties for nested suites
    */
    private static void setSuiteProperties(Properties p, String suiteName,
        Properties parentProperties, boolean isParent, boolean isTop)
        throws ClassNotFoundException, IOException
    {
        // Some properties may have been set by the top suite
        // jvm, jvmflags, classpath, systemdiff, verbose, etc.
        // In that case, these will be preserved for the rest
        if (jversion != null)
            p.put("jvm", jversion);
    	if ( jvmName == null )
    		jvmName = "currentjvm";
    	else
    		p.put("jvm", jvmName);
        
        if ( javaCmd == null )
            javaCmd = "java";
        else
            p.put("javaCmd", javaCmd);
    	if ( jvmflags != null )
    		p.put("jvmflags", jvmflags);
    	if ( classpath != null )
    		p.put("classpath", classpath);
    	if ( classpathServer != null )
    		p.put("classpathServer", classpathServer);
        if ( systemdiff != null )
            p.put("systemdiff", systemdiff);
        if ( verbose == true )
            p.put("verbose", "true");
        if ( bootcp != null )
            p.put("bootcp", "bootcp");
        if ( canondir != null )
            p.put("canondir", canondir);
            
		if ( (outputdir == null) || (outputdir.length() == 0) )
		{
		    outputdir = p.getProperty("outputdir");
		    if (outputdir == null)
		        outputdir = userdir;
		}

	    // framework may be set at the top, or just
	    // set for individual suites
	    if ( parentProperties.getProperty("framework") != null )
		    p.put("framework", framework);
		else
            framework = p.getProperty("framework");

		// same for serverJvm 
        if ( parentProperties.getProperty("serverJvm") != null )
            p.put("serverJvm", serverJvm);
		else
            serverJvm = p.getProperty("serverJvm");

        // Encryption may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("encryption") != null )
		    p.put("encryption", encryption);
		else
            encryption = p.getProperty("encryption");
        
	// Encryption provider may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("testEncryptionProvider") != null )
		    p.put("testEncryptionProvider", testEncryptionProvider);
		else
            testEncryptionProvider = p.getProperty("testEncryptionProvider");
        
        // jdk12test may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("jdk12test") != null )
		    p.put("jdk12test", jdk12test);
		else
            jdk12test = p.getProperty("jdk12test");
       
        // jdk12exttest may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("jdk12exttest") != null )
		    p.put("jdk12exttest", jdk12exttest);
		else
            jdk12exttest = p.getProperty("jdk12exttest");
        
        // jdk14test may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("jdk14test") != null )
		    p.put("jdk14test", jdk14test);
		else
            jdk14test = p.getProperty("jdk14test");
       
        // runwithibmjvm may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("runwithibmjvm") != null )
		    p.put("runwithibmjvm", runwithibmjvm);
		else
            runwithibmjvm = p.getProperty("runwithibmjvm");
       
        // runwithjvm may be set at the top or just for a subsuite
	    String testJVM = (jvmName.startsWith("j9") ? "j9" : jvmName);
	    if ( parentProperties.getProperty("runwith" + testJVM) != null )
		    p.put("runwith" + testJVM, runwithjvm);
		else
            runwithjvm = p.getProperty("runwith" + testJVM);

        // runwithj9 may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("runwithj9") != null )
		    p.put("runwithj9", runwithj9);
		else
            runwithj9 = p.getProperty("runwithj9");

        // excludeJCC may be set at the top or just for a subsuite
	    if ( parentProperties.getProperty("excludeJCC") != null )
		    p.put("excludeJCC", excludeJCC);
		else
            excludeJCC = p.getProperty("excludeJCC");
       
        // useprocess may be set at the top or just for a subsuite
        String upr = parentProperties.getProperty("useprocess");
	    if ( upr != null )
		    p.put("useprocess", upr);
		else
		{
            upr = p.getProperty("useprocess");
            if ( upr == null)
                useprocess = true;
            else if (upr.equals("false"))
                useprocess = false;
            else
                useprocess = true;
        }
		// properties specific to a single suite
		usesystem = p.getProperty("usesystem");
		shutdownurl = p.getProperty("shutdownurl");
        upgradetest = p.getProperty("upgradetest");
        jarfile = p.getProperty("jarfile");
        skipsed = p.getProperty("skipsed");
        if (skipsed == null)
            skipsed = "false";
		if ( "true".equals(keepfiles) )
		    p.put("keepfiles", keepfiles);
            
        // testJavaFlags should get appended
        
        String testflags = p.getProperty("testJavaFlags");
        if ( parentProperties.getProperty("testJavaFlags") != null )
        {
            if ( (testflags != null) && (!testflags.equals(testJavaFlags)) )
            {
                testJavaFlags = testJavaFlags + "^" + testflags;
            }
            p.put("testJavaFlags", testJavaFlags);
        }
        else
            testJavaFlags = p.getProperty("testJavaFlags");
            
		// The following could change between suites or
		// may be set for the whole set of suites

        if ( parentProperties.getProperty("reportstderr") != null )
            p.put("reportstderr", reportstderr);
        else
            reportstderr = p.getProperty("reportstderr");
            
        if ( parentProperties.getProperty("timeout") != null )
            p.put("timeout", timeout);
        else
            timeout = p.getProperty("timeout");
                   
        // outcopy is very specific to a single suite
        outcopy = p.getProperty("outcopy");
        
		// useoutput is very specific to a single suite
		useoutput = p.getProperty("useoutput");


        // mtestdir is very specific to a multi suite
        mtestdir = p.getProperty("mtestdir");

        // ijdefaultResourcePackage is specific for a suite
        ijdefaultResourcePackage = p.getProperty("ij.defaultResourcePackage");

		if ( topSuiteName == null )
		    topSuiteName = p.getProperty("suitename");
		else
		    p.put("suitename", topSuiteName);

		skip = shouldSkipTest();
		    
        // Set the suite subdir under top outputdir
        setSuiteDir(suiteName, isParent, isTop);
        
        // This individual suite may also have special flags
        // Reset otherSpecialProps in case another suite had any set
        otherSpecialProps = "";
        Properties specialProps = SpecialFlags.getSpecialProperties(p);
        if ( (specialProps != null) && (!specialProps.isEmpty()) )
            // Add any special properties to suiteJavaFlags string
            setSpecialProps(specialProps, false);
    }

	/**
		Determine if a test should be skipped or not.
		These are ad-hoc rules, see comments within for details.
		Examples of what is checked: JVM version, framework,
		encryption, jdk12test, 
		Sets some global variables so that skip reporting is clearer.

		@return true if test should not be run.
    */
    private static boolean shouldSkipTest()
    {
	boolean result = false;

	// figure out if suite should be skipped ... adhoc rules
	boolean isRmiJdbc = false;
	boolean isIBridge = false;
	boolean isJdk12 = false; // really now 'isJdk12orHigher'
	boolean isJdk14 = false;
	boolean isJdk118 = false;
	boolean isJdk117 = false;
	boolean isEncryption = false;
	boolean isJdk12Test = false;
	boolean isJdk12ExtTest = false;
	boolean isJdk14Test = false;
	boolean isSyncTest = false;
	boolean isSyncProduct = false;
	boolean isExcludeJCC = false;
	// runwithibmjvm is really tri-state. null = run-anywhere,
	// true = only ibm jvms, false = only non-IBM jvms.

	// reset skip reason parameters
	driverNotFound = false;
	needSync = false;
	needJdk12 = false;
	needJdk12ext = false;
	needJdk14 = false;
	excludedFromJCC = false;
	needIBMjvm = null;
        
	// Determine if this is jdk12 or higher (with or without extensions)
        if (iminor >= 2) isJdk12 = true;
	if ( System.getProperty("java.version").startsWith("1.1.8") ) isJdk118 = true;
        if ( System.getProperty("java.version").startsWith("1.1.7") ) isJdk117 = true;
        if ( System.getProperty("java.version").startsWith("1.4.") ) isJdk14 = true;

        if ( (framework != null) && (framework.length()>0) )
	{
            if (framework.equals("RmiJdbc"))
	    { 
		try {
			Class.forName("org.objectweb.rmijdbc.Driver");
		} catch (ClassNotFoundException cnfe) {
			driverNotFound = true;
			result = true;
		}
            }
            else if (framework.equals("DerbyNet"))
	    {
		try {
			Class.forName("org.apache.derby.drda.NetworkServerControl");
		} catch (ClassNotFoundException cnfe) {
			driverNotFound = true;
			result = true;
		}
	    }
	}

	if (result) return true; // stop looking once know should skip

        if ( (encryption != null) && (encryption.length()>0) )
            if ("true".equalsIgnoreCase(encryption)) isEncryption = true;
        if ( (jdk12test != null) && (jdk12test.length()>0) )
            if ("true".equalsIgnoreCase(jdk12test)) isJdk12Test = true;		
        if ( (jdk12exttest != null) && (jdk12exttest.length()>0) )
            if ("true".equalsIgnoreCase(jdk12exttest)) isJdk12ExtTest = true;
        if ( (jdk14test != null) && (jdk14test.length()>0) )
            if ("true".equalsIgnoreCase(jdk14test)) isJdk14Test = true;		

        // Skip any suite if jvm is not jdk12 or higher for encryption, jdk12test or jdk12exttest
        if (!isJdk12)
        {
            if ( (isEncryption) || (isJdk12Test) || (isJdk12ExtTest) )
            {
                needJdk12 = true;
                result = true; // Can't run in this combination
            }
	    if (result) return true; // stop looking once know should skip
 	}		

        // Skip any suite if jvm is not jdk14 or higher for jdk14test
        if (!isJdk14 &&  isJdk14Test)
	{
		needJdk14 = true;
  		return true;
        }
	
        // Also require jdk12 extensions for encryption and jdk12exttest
	if ( (isEncryption) || (isJdk12ExtTest) )
	{
	    needJdk12ext = true;
            // Check for extensions
            try
            {
                Class jtaClass = Class.forName("javax.transaction.xa.Xid");
                Class jdbcClass = Class.forName("javax.sql.RowSet");
            } 
            catch (ClassNotFoundException cnfe)
            {
                // at least one of the extension classes was not found
                result = true; // skip this test
            }			
	    if (result) return true; // stop looking once know should skip
        }

        if (isEncryption)  // make sure encryption classes are available
        {
            needEncryption = true;
            try 
            {
                Class jceClass = Class.forName("javax.crypto.Cipher");
            }
            catch (ClassNotFoundException cnfe)
            {
                result = true;
            }
            if (result) return true;
        }

	// if a test needs an ibm jvm, skip if runwithibmjvm is true.
	// if a test needs to not run in an ibm jvm, skip if runwithibmjvm is false.
	// if null, continue in all cases.
	if (runwithibmjvm != null) 
	{ 
	    if (runwithibmjvm.equals("")) { needIBMjvm = null; }
	    else { needIBMjvm = new Boolean(runwithibmjvm); }
	}
	if (runwithibmjvm == null) { needIBMjvm = null; }
	if (needIBMjvm != null)
	{
	    boolean needsibm = needIBMjvm.booleanValue();
	    boolean ibmjvm = false;
	    String vendor = System.getProperty("java.vendor");
	    if (vendor.startsWith("IBM")) { ibmjvm = true; }
	    if (!needsibm && ibmjvm) { return true; }
	    if (needsibm && !ibmjvm) { return true; }
	}

	if (runwithjvm != null && runwithjvm.equals("false"))
	{
	    return true;
	}
	if (runwithj9 != null && runwithj9.equals("false"))
	{
	    return false ;
	}

	if (excludeJCC != null)
	{
	    Class c = null;
	    Method m = null;
	    Object o = null;
	    Integer i = null;
	    int jccMajor = 0;
	    int jccMinor = 0;
	    try	
	    {
		c = Class.forName("com.ibm.db2.jcc.DB2Driver");
		o = c.newInstance();
		m = c.getMethod("getMajorVersion", null);
		i = (Integer)m.invoke(o, null);
		jccMajor = i.intValue();
		m = c.getMethod("getMinorVersion", null);
		i = (Integer)m.invoke(o, null);
		jccMinor = i.intValue();
	    } catch (Exception e) {
	        if (verbose) System.out.println("Exception in shouldSkipTest: " + e);
            }

	    if (excludeJCC != null) {
		int excludeMajor = 0;
		int excludeMinor = 0;
		try 
		{
		    excludeMajor = Integer.parseInt(excludeJCC.substring(0,excludeJCC.indexOf(".")));
		    excludeMinor = Integer.parseInt(excludeJCC.substring(excludeJCC.indexOf(".")+1));
		} catch (NumberFormatException nfe) {
		    System.out.println("excludeJCC property poorly formatted: " + excludeJCC);
		} catch (NullPointerException npe) {
		    System.out.println("excludeJCC property poorly formatted: " + excludeJCC);
		}
		if (excludeMajor >= jccMajor && excludeMinor >= jccMinor)
		{
		    excludedFromJCC = true;
		    return true;
		}
	    }
	} 

	return result; // last test result is returned
    }

    
    public static void setSuiteDir(String suiteName, boolean isParent, boolean isTop)
        throws IOException
    {
        if (isTop) // This is the very top suite for this RunList
        {
            // Here we want to set the topsuitedir
		    if ( (topsuitedir == null) || (topsuitedir.length() == 0) )
		    {
		        topsuitedir = userdir;
		        outputdir = topsuitedir;
		    }
		    else
		        outputdir = topsuitedir;
		    
		    // Create the topsuite directory under the outputdir
		    File topdir = new File(outputdir, topSuiteName);
		    topdir.mkdir();
		    if (!topParentSuite.equals(topSuiteName))
		    {
		        File topparent = new File(topdir, topParentSuite);
                topparent.mkdir();
                outputdir = topparent.getCanonicalPath();
            }
            else
                outputdir = topdir.getCanonicalPath();
            topreportdir = outputdir;
            //System.out.println("RunList topsuitedir: " + outputdir);
            //System.out.println("RunList outputdir: " + outputdir);
            //System.out.println("RunList topreportdir: " + topreportdir);
            
            // Modify outputdir for special framework
            if ( (framework != null) && (framework.length()>0) )
            {
                File f = new File(outputdir, framework);
                f.mkdir();
                outputdir =  f.getCanonicalPath();
		        fw_set = true; // framework dir set at top level
                //System.out.println("RunList for framework outputdir: " + outputdir);
            }
		    topsuitedir = outputdir; 	
		}
		else if (isParent) // reset outputdir to topsuitedir for a new parent
		{
		    outputdir = topsuitedir;
		    //System.out.println("outputdir reset for parent: " + outputdir);
		    if (!suiteName.equals(topParentSuite))
		    {
                File suitedir = new File(outputdir, suiteName);
                suitedir.mkdir();
                outputdir = suitedir.getCanonicalPath();
            }
            // Modify outputdir for special framework (if not already set)
            if (!fw_set)
            {
                if ( (framework != null) && (framework.length()>0) )
                {
                    File f = new File(outputdir, framework);
                    f.mkdir();
                    outputdir =  f.getCanonicalPath();
                }
            }
        }
        
		else if ( upgradetest == null ) // this is a child suite of a parent
		{
            File suitedir = new File(outputdir, suiteName);
            suitedir.mkdir();
            outputdir = suitedir.getCanonicalPath();
            //System.out.println("Child outputdir: " + outputdir);
        }
    }
    
	private static void setSpecialProps(Properties p, boolean isTop)
	{
        // Just build  string for RunTest to parse (^ is the separator)
        // and determine which special flags are for ij or for server
        // These special flags come from specialProperties, not from
        // the usual properties (RunSuite will give these for the top suite)
        String tmp = "";
		for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			// Note: RunSuite will already have excluded
			// suites, useoutput, usesystem,keepfiles from these
			tmp += key + "=" + p.getProperty(key) + "^";
		}
		if (tmp.length()>0)
		{
		    if ( isTop == true ) // This is the top level suite
		        topSpecialProps = tmp.substring(0, tmp.lastIndexOf('^'));
		    else // This is a nested suite, do not apply to all the suites
		        otherSpecialProps = tmp.substring(0, tmp.lastIndexOf('^'));
		}
	}
	
	static void addToSkipFile(String suiteName, PrintStream ps) throws IOException
	{
		ps.println(suiteName);
		ps.flush();
    }
	
}
