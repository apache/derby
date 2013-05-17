/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun
 
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
package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.io.*;
import java.util.List;


/**
 * Utilities for replication test framework:
 * cleaning directories, copying directories for 
 * test databases.
 * Debug printing.
 */

class Utils
{
    boolean printDebug = false;
    
    private final static String FS = File.separator;
    
    ///////////////////////////////////////////////////////////////////////////
    // File utilities:
    
    /**
     * Copy directory sourcePath into directory destPath
     * @param sourcePath Directory to copy
     * @param destPath Directory to copy into
     * @throws IOException If copying failed.
     */
    void copyDir(String sourcePath, String destPath)
    throws IOException
    {
        DEBUG("copyDir from " + sourcePath + " to " + destPath);
        copyFiles(sourcePath,destPath);
    }
   /** Copy files
    * @param srcPath Directory or file to copy from
    * @param destPath Directory or file to copy to
    * @throws IOException If copying failed.
    */
    void copyFiles(String srcPath, String destPath) 
    throws IOException
    {
        File src = new File(srcPath);
        File dest = new File(destPath);
        
        if (src.isDirectory())
        {
            DEBUG("Make dir: " + dest.getAbsolutePath());
            dest.mkdirs();
            String list[] = src.list();
            for (int i = 0; i < list.length; i++)
            {
                String srcFile = src.getAbsolutePath() + FS + list[i];
                String destFile = dest.getAbsolutePath() + FS + list[i];
                // DEBUG("Copy " + srcFile + " to " + destFile);
                copyFiles(srcFile , destFile);
            }
        }
        else
        {
            copy(src,dest);  // Also works w/ JVM 1.4
            // NIOcopy(src,dest); // Requires JVM 1.5 or 1.6
        }
    }
    private void copy(File source, File dest) 
    throws IOException
    {
        // DEBUG("Copy file " + source.getAbsolutePath() + " to " + dest.getAbsolutePath());
        FileInputStream src = new FileInputStream(source);
        FileOutputStream dst = new FileOutputStream(dest);
        int c;
        while ((c = src.read()) >= 0)
            dst.write(c);
        src.close();
        dst.close();
    }
    
    void writeToFile(String text, String outFile)
    throws IOException
    {
        DEBUG("writeToFile " + outFile);
        FileWriter out = new FileWriter(outFile);
        out.write(text);
        out.close();
    }

    void mkDirs(String dirPath)
    {
        File dir = new File(dirPath);
        dir.mkdirs();
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    // Debug
        
    void DEBUG(String s)
    {
        if ( printDebug )
            System.out.println(s);
    }
    void DEBUG(String s, PrintWriter out)
    {
        if ( printDebug )
            out.println(s);
    }
    
    // Sleep w/Debug...
    void sleep(long sleepTime, String ID) 
    throws InterruptedException
    {
        DEBUG(ID + ": sleep " + sleepTime + "ms.");
        Thread.sleep(sleepTime);
    }

    /**
     * Convert a list of strings to an array of strings.
     */
    String[] toStringArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }

    /**
     * Splice together strings. Separate the sub-strings by the specified
     * separator character.
     */
    String splice(String[] strings, char sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) sb.append(sep);
            sb.append(strings[i]);
        }
        return sb.toString();
    }
}
