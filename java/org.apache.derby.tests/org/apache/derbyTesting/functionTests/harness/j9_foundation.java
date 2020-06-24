/*

   Derby - Class org.apache.derbyTesting.functionTests.harness.j9_foundation

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
import java.util.Vector;
import java.util.StringTokenizer;
import java.util.Properties;

/**
  <p>This class is for IBM's J9 jdk 1.3., foundation class library; v 2.2 (wctme5.7)

 */
public class j9_foundation extends jvm {

    public String getName(){return "j9_foundation";}
    public j9_foundation(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
    long ss, long oss, long ms, long mx, String classpath, String prof,
    boolean verify, boolean noverify, boolean nojit, Vector<String> D) {
        super(noasyncgc,verbosegc,noclassgc,ss,oss,ms,mx,classpath,prof,
		verify,noverify,nojit,D);
    }
    // more typical use:
    public j9_foundation(String classpath, Vector<String> D) {
        super(classpath,D);
    }
    // more typical use:
    public j9_foundation(long ms, long mx, String classpath, Vector<String> D) {
        super(ms,mx,classpath,D);
    }
    // actual use
    public j9_foundation() {
	Properties sp = System.getProperties();
	String srvJvm = sp.getProperty("serverJvm");
	if ((srvJvm!=null) && (srvJvm.toUpperCase().startsWith("J9")))
	{
		String wshome = guessWSHome();
		// note, may have to use separator instead of hardcoding the slashes...
		setJavaCmd(wshome+"/wctme5.7/ive/bin/j9");
	}
	else
		setJavaCmd("j9");
    }

    // return the command line to invoke this VM.  The caller then adds
    // the class and program arguments.
    public Vector<String> getCommandLine()
    {
        StringBuffer sb = new StringBuffer();
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Vector<String> v = super.getCommandLine();
        appendOtherFlags(sb);
        String s = sb.toString();
        StringTokenizer st = new StringTokenizer(s);
        while (st.hasMoreTokens())
        {
            v.addElement(st.nextToken());
        }
        return v;
	}

    public void appendOtherFlags(StringBuffer sb)
    {
        Properties sp = System.getProperties();
        String bootcp = sp.getProperty("bootcp");
        String srvJvm = sp.getProperty("serverJvm");
        // if we're coming in to be the server jvm for networkserver testing on j9,
        // bootcp is null, so we need to try to setup the bootclasspath from scratch
        // for now, assume we're only interested in doing this for wctme5.7_foundation, worry about
        // newer versions, multiple class libraries, or multiple releases later.
        sb.append(" -jcl:foun10");

        if ((srvJvm !=null ) && (srvJvm.toUpperCase().startsWith("J9")))
        {
            String pathsep = System.getProperty("path.separator");
            String wshome = guessWSHome();
            // note, assuming jclFoundation classes sit under wctme5.7/ive/lib/jclFoundation10
            //       and that jdbc.jar sits under wctme5.7/ive/lib
            // note, may have to switch to sep instead of hardcoding the slashes...
            sb.append(" -Xbootclasspath/a:" + wshome + "/wctme5.7/ive/lib/jclFoundation10/classes.zip"
                + pathsep + wshome + "/wctme5.7/ive/lib/jclFoundation10/locale.zip"
                + pathsep + wshome + "/wctme5.7/ive/lib/jdbc.jar");
        } 
        else
            sb.append(" -Xbootclasspath/a:" + bootcp);
        if (noasyncgc) warn("j9_foundation does not support noasyncgc");
        if (verbosegc) sb.append(" -verbose:gc");
        if (noclassgc) warn("j9_foundation does not support noclassgc");
        if (ss>=0) warn("j9_foundation does not support ss");
        if (oss>=0) warn("j9_foundation does not support oss");
        if (ms>=0) {
          sb.append(" -Xss");
          sb.append(ms);
		  //sb.append("k");
        }
        if (mx>=0) {
          sb.append(" -Xmx");
          sb.append(mx);
		  //sb.append("k");
        }
        if (classpath!=null) warn("j9_foundation does not support classpath, use -Xbootclasspath,-Xbootclasspath/p,-Xbootclasspath/a"); 
        if (prof!=null) warn("j9_foundation does not support prof");
        if (verify) sb.append(" -verify");
        if (noverify) warn("j9_foundation does not support noverify");
        if (nojit) sb.append(" -Xnojit");
        if (D != null)
          for (int i=0; i<D.size();i++) {
	        sb.append(" -D");
	        sb.append((String)(D.elementAt(i)));
          }
    }
	public String getDintro() { return "-D"; }

// Having the following method overload the one in jvm.java causes problems when running
// the junit tests - they *do* successfully run with securityManager.
// Foundation class tests actually run ok with security manager - except when useprocess
// is false. This is caused by a bug in the jvm. See also DERBY-885 and DERBY-1785.
//	protected void setSecurityProps()
//	{
//		System.out.println("Note: J9 (foundation) tests do not run with security manager");		
//	}

}
