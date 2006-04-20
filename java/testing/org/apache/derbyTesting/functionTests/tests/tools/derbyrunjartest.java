/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.derbyrunjartest

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.File;
import java.io.IOException;
import java.lang.Process;
import java.lang.Runtime;
import java.lang.SecurityException;
import java.net.URL;
import java.security.CodeSource;

import org.apache.derbyTesting.functionTests.harness.BackgroundStreamSaver;

public class derbyrunjartest {

    public static void main(String[] args) throws IOException
    {
        // get location of run class.
        CodeSource cs = null;
        try {
            cs = org.apache.derby.iapi.tools.run.class.getProtectionDomain().getCodeSource();
        } catch (SecurityException se) {
            System.out.println("Security exception: " + se.getMessage());
        }
 
        URL result = cs.getLocation();

        if (result.toString().endsWith(".jar")) {
            String derbyrunloc = result.toString().substring(5);
            if (System.getProperty("os.name").startsWith("Windows"))
              derbyrunloc = derbyrunloc.substring(1);
            runtool(derbyrunloc, "ij --help");
            runtool(derbyrunloc, "sysinfo -cp help");
            runtool(derbyrunloc, "dblook");
            runtool(derbyrunloc, "server");
        } else {
            String[] ij = {"ij", "--help"};
            System.out.println("ij --help:");
            org.apache.derby.iapi.tools.run.main(ij);

            String[] sysinfo = {"sysinfo", "-cp", "help"}; 
            System.out.println("sysinfo -cp help:");
            org.apache.derby.iapi.tools.run.main(sysinfo);

            String[] dblook = {"dblook"};
            System.out.println("dblook:");
            org.apache.derby.iapi.tools.run.main(dblook);

            String[] server = {"server"};
            System.out.println("server:");
            org.apache.derby.iapi.tools.run.main(server);
        }
    }

    private static void runtool(String loc, String tool)
    {
        String command = "java -jar " + loc + ' ' + tool;
        Process pr = null;

        System.out.println(tool + ':');
        try
        {
            pr = Runtime.getRuntime().exec(command);
            BackgroundStreamSaver saver = 
                        new BackgroundStreamSaver(pr.getInputStream(), System.out);
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
}
