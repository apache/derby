/*
 
   Derby - Class org.apache.derbyTesting.functionTests.util.JartUtil
 
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */

package org.apache.derbyTesting.functionTests.util;
import java.io.*;


/**
 *
 * <Put Class Comments Here>
 */
public class JarUtil {

    /** 
     * Unjar a file into the specified directory.  This runs in a separate
     * process.  Note, your test needs security permissions to read user.dir
     * and to start a process for this to work.
     * 
     * @param jarpath - Path to jar file
     *
     * @param outputdir - The directory to unjar to.  If this is null,
     *    we user user.dir (the current directory)
     *
     */
    public static void unjar(String jarpath, String outputdir)
        throws ClassNotFoundException, IOException, InterruptedException
    {                
        if ( outputdir == null ) {
            outputdir = System.getProperty("user.dir");
        }
        File jarFile = new File((new File(outputdir, jarpath)).getCanonicalPath());

        // Now unjar the file
        String jarCmd = "jar xf " + jarFile.getPath();
        // Now execute the jar command
        Process pr = null;
        try
        {
            //System.out.println("Use process to execute: " + jarCmd);
            pr = Runtime.getRuntime().exec(jarCmd);

            pr.waitFor();
            //System.out.println("Process done.");
            pr.destroy();
        }
        finally {
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
    }
}