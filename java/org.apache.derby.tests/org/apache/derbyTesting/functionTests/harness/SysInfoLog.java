/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.SysInfoLog

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

package org.apache.derbyTesting.functionTests.harness;

/***
 * SysInfoLog
 * Purpose: For a Suite or Test run, write out the
 * sysinfo to the suite or test output file
 *
 ***/

import java.io.*;
import java.util.Vector;

import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;

public class SysInfoLog
{

    public SysInfoLog()
    {
    }

    // Write out sysinfo for a suite or test
    public void exec(String jvmName, String javaCmd, String classpath,
        String framework, PrintWriter pw, boolean useprocess)
        throws Exception
	{
        boolean isModuleAware = JVMInfo.isModuleAware();
      
        if ( useprocess == true )
        {
            // Create a process to run sysinfo
    		Process pr = null;
			jvm javavm = null; // to quiet the compiler
    		try
    		{
                // Create the command line
                System.out.println("jvmName: " + jvmName);
                if ( (jvmName == null) || (jvmName.length()==0) )
                    jvmName = "jdk13";
                else if (jvmName.startsWith("jdk13"))
                    jvmName = "jdk13";

				javavm = jvm.getJvm(jvmName);
                if (javaCmd != null)
                    javavm.setJavaCmd(javaCmd);
				
                if (javavm == null) System.out.println("WHOA, javavm is NULL");
                if (javavm == null) pw.println("WHOA, javavm is NULL");

                if ( (classpath != null) && (classpath.length()>0) )
                {
                    javavm.setClasspath(classpath);
                }

				Vector<String> v = javavm.getCommandLine();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                String mainClassName = "org.apache.derby.tools.sysinfo";
                if (isModuleAware)
                {
                    v.add("-m");
                    v.add(ModuleUtil.TOOLS_MODULE_NAME + "/" + mainClassName);
                }
                else
                {
                    v.addElement(mainClassName);
                }
                
                // Now convert the vector into a string array
                String[] sCmd = new String[v.size()];
                for (int i = 0; i < v.size(); i++)
                {
                    sCmd[i] = (String)v.elementAt(i);
                    //System.out.println(sCmd[i]);
                }

                pr = Runtime.getRuntime().exec(sCmd);

                // We need the process inputstream to capture into the output file
                BackgroundStreamDrainer stdout =
                    new BackgroundStreamDrainer(pr.getInputStream(), null);
                BackgroundStreamDrainer stderr =
                    new BackgroundStreamDrainer(pr.getErrorStream(), null);

                pr.waitFor();
                String result = HandleResult.handleResult(pr.exitValue(),
                    stdout.getData(), stderr.getData(), pw);
                pw.flush();

                if ( (framework != null) && (framework.length()>0) )
                {
                    pw.println("Framework: " + framework);
                }

                pr.destroy();
                pr = null;
            }
            catch(Throwable t)
            {
                if (javavm == null) System.out.println("WHOA, javavm is NULL");
                if (javavm == null) pw.println("WHOA, javavm is NULL");
                System.out.println("Process exception: " + t);
                pw.println("Process exception: " + t);
                t.printStackTrace(pw);
                if (pr != null)
                {
                    pr.destroy();
                    pr = null;
                }
            }
        }
        else
        {
            // For platforms where process exec fails or hangs
            // useprocess=false and attempt to get some info
            /*
            pw.println(org.apache.derby.impl.tools.sysinfo.Main.javaSep);
            org.apache.derby.impl.tools.sysinfo.Main.reportCloudscape(pw);
            pw.println(org.apache.derby.impl.tools.sysinfo.Main.jbmsSep);
//IC see: https://issues.apache.org/jira/browse/DERBY-920
            org.apache.derby.impl.tools.sysinfo.Main.reportDerby(pw);
            pw.println(org.apache.derby.impl.tools.sysinfo.Main.licSep);
            org.apache.derby.impl.tools.sysinfo.Main.printLicenseFile(pw);
            */
        }
    }
}

