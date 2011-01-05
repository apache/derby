/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.OldVersions

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

/**
 * <p>
 * Old versions visible to the upgrade machinery.
 * </p>
 */
public class OldVersions
{
    private static int[][] VERSIONS =
    {
        {10, 0, 2, 1}, // 10.0.2.1 (incubator release)
        {10, 1, 1, 0}, // 10.1.1.0 (Aug 3, 2005 / SVN 208786)
        {10, 1, 2, 1}, // 10.1.2.1 (Nov 18, 2005 / SVN 330608)
        {10, 1, 3, 1}, // 10.1.3.1 (Jun 30, 2006 / SVN 417277)
        {10, 2, 1, 6}, // 10.2.1.6 (Oct 02, 2006 / SVN 452058)
        {10, 2, 2, 0}, // 10.2.2.0 (Dec 12, 2006 / SVN 485682)
        {10, 3, 1, 4}, // 10.3.1.4 (Aug 1, 2007 / SVN 561794)
        {10, 3, 3, 0}, // 10.3.3.0 (May 12, 2008 / SVN 652961)
        {10, 4, 1, 3}, // 10.4.1.3 (April 24, 2008 / SVN 648739)
        {10, 4, 2, 0}, // 10.4.2.0 (September 05, 2008 / SVN 693552)
        {10, 5, 1, 1}, // 10.5.1.1 (April 28, 2009 / SVN 764942)
        {10, 5, 3, 0}, // 10.5.3.0 (August 21, 2009 / SVN 802917)
        {10, 6, 1, 0}, // 10.6.1.0 (May 18, 2010/ SVN 938214)
        {10, 6, 2, 1}, // 10.6.2.1 (Oct 6, 2010/ SVN 999685
    };

    //Constant for special upgrade testing with both upgrade and create 
    // set. We just test this with one version in the interest of time
    // DERBY-4913
    public static int[] VERSION_10_3_3_0=  new int[] {10,3,3,0};
    /**
     * <p>
     * Get an array of versions supported by this platform.
     * </p>
     */
    public static int[][] getSupportedVersions()
    {
        int[][] old = null;
        
        if ( UpgradeRun.oldVersionsPath != null )
        {
            old = getVersions(UpgradeRun.oldVersionsPath);
        }
        
        if ( old == null ) { old = VERSIONS; }

        show( old );

        return getSupportedVersions( old );
    }

    /**
     * <p>
     * Squeeze the supported versions out of any array of candidate versions.
     * </p>
     */
    private static int[][] getSupportedVersions( int[][] old )
    {
        ArrayList list = new ArrayList();
        int count = old.length;
        
        for (int i = 0; i < count; i++) {
            // JSR169 support was only added with 10.1, so don't
            // run 10.0 to later upgrade if that's what our jvm is supporting.
            if ((JDBC.vmSupportsJSR169() && 
                (old[i][0]==10) && (old[i][1]==0))) {
                traceit("Skipping 10.0 on JSR169");
                continue;
            }
            // Derby 10.3.1.4 does not boot on the phoneME advanced platform,
            // (see DERBY-3176) so don't run upgrade tests in this combination.
            if ( System.getProperty("java.vm.name").equals("CVM")
                  && System.getProperty("java.vm.version").startsWith("phoneme")
                  && old[i][0]==10 && old[i][1]==3 
                  && old[i][2]==1 && old[i][3]==4 ) {
                traceit("Skipping 10.3.1.4 on CVM/phoneme");
                continue;
            }

            // otherwise, it's a supported version
            list.add( old[ i ] );
        }

        int[][] result = new int[ list.size() ][ 4 ];
        list.toArray( result );

        return result;
    }
    
    private static int[][] getVersions(String oldVersionsPath)
    {
        BufferedReader br = null;
        try{
            FileReader fr = new FileReader(oldVersionsPath);
            br = new BufferedReader(fr);
        }
        catch (java.io.FileNotFoundException fNFE)
        {
            alarm("File '" + oldVersionsPath 
                  + "' was not found, using default old versions for upgrade tests.");
            return null;
        }
        traceit("Run upgrade tests on versions defined in '" + oldVersionsPath + "'");
        return getVersions(br, oldVersionsPath);
    }
    
    private static int[][] getVersions(BufferedReader br, String oldVersionsPath) 
    {
        int[][] versionArray = new int[256][4];
        
        int versions = 0;
        
        String line = null;
        int lineNum = 0;
        try {
            while ((line = br.readLine()) != null) {
                lineNum++;
                /* Ignore lines not matching the regexp: "^[\\d]+\\.[\\d]+\\.[\\d]+\\.[\\d]"
                 * NB. java.util.regex.Matcher and java.util.regex.Pattern can not be
                 * used on small devices(JSR219).
                 */
                try {
                    String[] parts = split4(line,'.');
                    // String[] parts = line.split("\\."); // JSR219 does NOT have String.split()!
                    if (parts.length >= 3) {
                        
                        int[] vstr = new int[4];
                        for (int i = 0; i < 4; i++) // Using first 4 values
                        {
                            String str = parts[i];
                            if (i == 3) { // Clean... remove trailing non-digits
                                str = clean(str,"0123456789");
                            }
                            vstr[i] = Integer.parseInt(str);
                        }
                        versionArray[versions++] = vstr;
                    } else {
                        alarm("Illegal version format on: " + line);
                    }
                } catch (NumberFormatException nfe) {
                    alarm("NumberFormatException on line " + lineNum + ": " + line + ": " + " " + nfe.getMessage());
                } catch (ArrayIndexOutOfBoundsException aie) {
                    alarm("ArrayIndexOutOfBoundsException on line " + lineNum + ": " + line + ": " + " " + aie.getMessage());
                }
            }
        } catch (IOException ioe) {
            alarm("Error reading from file: " + oldVersionsPath + ioe.getMessage());
        }
        
        int[][] finalVERSIONS = new int[versions][4];
        for (int v = 0; v < versions; v++) {
            finalVERSIONS[v] = versionArray[v];
        }
        return  finalVERSIONS;
        
    }
    
    private static void show( int[][] old ) {
        traceit("Upgrade test versions listed:");
        for (int o = 0; o < old.length; o++) {
            String ver = "";
            for (int i = 0; i < old[o].length; i++) {
                if (i == 0) {
                    ver = "" + old[o][i];
                } else {
                    ver = ver + "." + old[o][i];
                }
            }
            traceit(ver);
        }
    }
    private static String[] split4(String l, char c)
    {
        String[] res = new String[4];
        try{
            int p0 = l.indexOf(c);
            if (p0<0) return res;
            
            res[0] = l.substring(0, p0);
            int p1 = l.indexOf(c,p0+1);
            if (p1<0) return res;
            
            res[1] = l.substring(p0+1, p1);
            int p2 = l.indexOf(c,p1+1); 
            if (p2<0) return res;
            
            res[2] = l.substring(p1+1, p2);
            int p3 = l.indexOf(c,p2+1); 
            if (p3<0) p3=l.length();
            
            res[3] = l.substring(p2+1, p3);
            
        } catch(StringIndexOutOfBoundsException sie){
            println("split4 StringIndexOutOfBoundsException: "+sie.getMessage());
            sie.printStackTrace();
        }
        return res;
    }
    private static String clean(String l, String allowed)
    {
        for (int i=0;i<l.length();i++)
        {
            if (!matches(l.charAt(i),allowed))
            {
                return l.substring(0,i);
            }
        }
        return l;
    }
    private static boolean matches(char c, String allowed)
    {
        for (int j=0;j<allowed.length();j++)
        {
            if (allowed.charAt(j) == c) return true;
        }
        return false;
    }

    private static void println( String text ) { BaseTestCase.println( text ); }
    private static void traceit( String text ) { BaseTestCase.traceit( text ); }
    private static void alarm( String text ) { BaseTestCase.alarm( text ); }
    
}
