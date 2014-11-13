/*
 
   Derby - Class org.apache.JarDriftTest
 
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

package org.apache.derbyBuild;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/* Notes:
 * -- this is run as the last depends in the ant buildjars target.
 * -- the jar file location is taken relative to that position and not
 *    related to the classpath setting.
 * -- we check on sane or insane by looking in the file 
 *    generated/java/org/apache/derby/shared/common/sanity/state.properties
 * -- source file and '*lastcontents files will be in the source distro,
 *    but they do not need to go in any jar files.
 * -- presence of derbyTesting.jar is optional. 
 * -- to update the lastcontents list when a new class is being added,
 *    run this class with the 'refresh' option, or ant refreshjardriftcheck
 * -- this implementation should work with just 1.6, although then 
 *    a number of classes (Java 8 support) will not get built. So only
 *    run the refresh with the highest level jvm.
 * -- we are only worrying about class files, even ignoring inner classes
 * -- we're not unravelling any jars from inside the jars
 */

public class JarDriftTest {
    static boolean failbuild = false;
    
    /**
     * <p>
     * Like the MessageBundleTest, let Ant conjure us up.
     * </p>
     */
    public JarDriftTest()
    {}

    protected PrintWriter pwOut;
    protected Hashtable<String, Object> classpathHash;
    // if run with -Dverbose=true
    protected static boolean verbose = Boolean.getBoolean("verbose");
    protected String classpath[] = null;
    
    static boolean updateFiles=false;
    static String sanityState;
    static String topOfTree;
    static String[] baseJars  = {"derby.jar", "derbyclient.jar", "derbynet.jar",
                               "derbytools.jar", "derbyoptionaltools.jar",
                               "derbyrun.jar", "derbyTesting.jar"};
    static HashSet<String> pastJarContents  = new HashSet<String>();
    static HashSet<String> builtJarContents = new HashSet<String>();
    
    public static void main(String [] args) throws Exception
    {
        if (args.length > 0 
                && args[0].equalsIgnoreCase("refresh"))
            updateFiles=true;
        JarDriftTest t = new JarDriftTest();
        // find local path
        topOfTree = System.getProperty("user.dir");
        if (verbose)
            System.out.println("top of build: " + topOfTree);
        sanityState = sanityState();
        try {
            for (int i = 0 ; i< baseJars.length ; i++) {
                t.jarCheck(baseJars[i]);
            }
        } catch (Exception e) {
            System.out.println("jar drift check failed: ");
            e.printStackTrace();
        }
        if (failbuild) 
            throw new Exception(
                "\njar drift check failed; see DERBY-6471 for info. \n" +
                "See error in build output or call ant jardriftcheck. \n" +
                "If the new class is expected run ant refreshjardriftcheck.\n"+
                "NB: Run the refresh for both sane and insane builds. \n" +
                "    Use the highest supported JVM (currently Java 8) \n " +
                "    to ensure all classes are built.\n");
    }    

    protected void loadJarContents(String jarName)
            throws Exception {
        if (verbose)
            System.out.println("jar: " + jarName);
        String jarFileLocation = topOfTree + File.separator + "jars";
        if (sanityState != null)
        {
            jarFileLocation=jarFileLocation + File.separator + sanityState;
            if (verbose)
                System.out.println(
                    "get the jar files from the dir: " + jarFileLocation);
        }
        else 
            System.out.println("oops sanity state is null, not sure why");
        // use 1.6 style code
        List<String> classNames=new ArrayList<String>();
        try { 
            ZipInputStream zip=new ZipInputStream(
                    new FileInputStream(jarFileLocation + File.separator + jarName));
            for (ZipEntry entry=zip.getNextEntry() ; entry!=null ; entry=zip.getNextEntry())
            {
                String className = entry.getName();
                if (className.endsWith(".class") && !entry.isDirectory()) {
                    // ignore classes with a '$' in the name - these are inner
                    // classes and it is possible that more get created if the 
                    // base class grows, or different compilers may add more
                    // and it seems unlikely one of the inner classes would 
                    // drift into another jar without its parent drifting.
                    if (className.contains("$"))
                        continue;
                    className = entry.getName().replace(File.separator, ".");
                    // replace some other possible separator chars in case the 
                    // character is OS, vendor, or zip implementation dependent.
                    className = className.replace("/", ".");
                    className = className.replace("//", ".");
                    className = className.replace("\\", ".");
                    classNames.add(className.toString());
                }
            }
        } catch (IOException ioe ) {
            // in all other cases, the process should/will fail but there is
            // a target to build without derbyTesting.jar, so it may be ok
            if (!jarName.equalsIgnoreCase("derbyTesting.jar"))
                throw ioe;
        }
        
        // load the contents into builtJarContents
        builtJarContents.addAll(classNames);
        
        // write out the contents ?
        if (updateFiles) {
            String fs = File.separator;
            String outputFile = topOfTree + fs + "java" + fs + "build" + fs +
                    "org" + fs + "apache" + fs + "derbyBuild" + fs + 
                    "lastgoodjarcontents" + fs +
                    sanityState + "." + jarName + ".lastcontents";
            PrintWriter pwf = new PrintWriter(outputFile);
            for (String className : classNames)
                pwf.println(className);
            pwf.close();
        }
    }
    
    protected static String sanityState() throws Exception {
        // need to check on sane vs. insane - access file
        // generated/java/org/apache/derby/shared/common/sanity/state.properties
        // and look for 'false' or 'true' (the file should only have 
        // sanity=[false|true] except for some header/timestamp info).
        String fs = File.separator;
        String sanityFileName = 
                topOfTree + fs + "generated" + fs + "java" + fs + "org" + fs +
                "apache" + fs + "derby" + fs + "shared" + fs + "common" + fs +
                "sanity" + fs + "state.properties";
        File sanityFile = new File(sanityFileName);
        String sanityContents = readFile(sanityFile);
        if (sanityContents != null && sanityContents.length()> 0)
        {
            if (sanityContents.contains("false"))
                sanityState = "insane";
            else if (sanityContents.contains("true"))
                sanityState = "sane";
        }
        else
        {
            throw new Exception ("oops, something wrong getting the sanity state");
        }
        return sanityState;
    }
    
    private static String readFile(File file) throws IOException{
        StringBuffer strbuf = new StringBuffer();
        
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                strbuf.append(line);
                strbuf.append("\n");
            }
            fr.close();
            /* if (verbose) {
                System.out.println("Contents of file:");
                System.out.println(strbuf.toString()); */
        } catch (IOException e) {
            e.printStackTrace();
        }
        return strbuf.toString();
    }
    
    protected void loadPastContents(String jarName) throws Exception {
        String fs = File.separator;
        String inputFile = topOfTree + fs + "java" + fs + "build" + fs +
                "org" + fs + "apache" + fs + "derbyBuild" + fs + 
                "lastgoodjarcontents"+ fs + 
                sanityState + "." + jarName + ".lastcontents";
        if (verbose)
            System.out.println("looking through " + inputFile);
        String pastJarContentsString = readFile(new File(inputFile));
        // split up the string at each \n
        BufferedReader br = 
                new BufferedReader(new StringReader(pastJarContentsString));
        String className = null;
        while ((className = br.readLine()) != null){
            if (verbose)
                System.out.println("found/added: " + className);
            pastJarContents.add(className);
        }
    }
    
    protected void jarCheck(String jarName) throws Exception {
        builtJarContents.clear();
        pastJarContents.clear();
        loadJarContents(jarName);
        loadPastContents(jarName);
        if (verbose)
            System.out.println("\nnow comparing for jar: " + jarName);
        Iterator it = builtJarContents.iterator();
        
        while ( it.hasNext() ) {
            String objectName = (String)it.next();
            if (objectName.contains("class"))
            {
                if (verbose)
                    System.out.println("objectname: " + objectName);
                if ( ! pastJarContents.contains(objectName) ) {
                    // Don't fail out on the first one, we want to catch
                    // all of them.  Just note there was a failure and continue
                    failbuild=true;
                    System.err.println("ERROR: class " + objectName + " in\n" +
                        "       " + jarName + " was not previously there.\n");
                 }
            }
        }
    }
}
