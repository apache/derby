/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.derbyrunjartest

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.File;
import java.io.IOException;
import java.lang.Process;
import java.lang.Runtime;
import java.lang.SecurityException;
import java.net.URL;
import java.security.CodeSource;
import java.util.Vector;

import org.apache.derbyTesting.functionTests.harness.BackgroundStreamSaver;
import org.apache.derbyTesting.functionTests.harness.jvm;

public class derbyrunjartest {

    public static void main(String[] args) throws Exception
    {
        // get location of run class.
        CodeSource cs = null;
        try {
            cs = org.apache.derby.iapi.tools.run.class.getProtectionDomain().getCodeSource();
        } catch (SecurityException se) {
            System.out.println("Security exception: " + se.getMessage());
        }
 
        URL result = cs.getLocation();
        jvm jvm = null;
        String derbyrunloc = null;

        if (result.toString().endsWith(".jar")) {
            derbyrunloc = result.toString().substring(5);
            if (System.getProperty("os.name").startsWith("Windows"))
              derbyrunloc = derbyrunloc.substring(1);
            jvm = jvm.getJvm("currentjvm"); // ensure compatibility
        }

        String[][] testCommands = new String[][] {
            {"ij", "--help"},
            {"sysinfo", "-cp", "help"},
            {"dblook"},
            {"server"},
        };

        for (int i = 0; i < testCommands.length; i++) {
            runtool(jvm, derbyrunloc, testCommands[i]);
        }
    }

    private static void runtool(jvm jvm, String loc, String[] args)
        throws IOException
    {
        System.out.println(concatenate(args) + ':');

        if (jvm == null) {
            org.apache.derby.iapi.tools.run.main(args);
            return;
        }

        Vector cmd = jvm.getCommandLine();
        cmd.addElement("-jar");
        cmd.addElement(loc);
        for (int i=0; i < args.length; i++) {
            cmd.addElement(args[i]);
        }
        String command = concatenate((String[]) cmd.toArray(new String[0]));

        Process pr = null;

        try
        {
            pr = Runtime.getRuntime().exec(command);
            BackgroundStreamSaver saver = 
                        new BackgroundStreamSaver(pr.getInputStream(), System.out);
            saver.finish();
            pr.waitFor();
            pr.destroy();
        } catch(Throwable t) {
            System.out.println("Process exception: " + t.getMessage());
            if (pr != null)
            {
                pr.destroy();
                pr = null;
            }
        }
    }

    private static String concatenate(String[] args) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < args.length; i++) {
            buf.append(args[i]);
            if (i + 1 < args.length) buf.append(' ');
        }
        return buf.toString();
    }
}
