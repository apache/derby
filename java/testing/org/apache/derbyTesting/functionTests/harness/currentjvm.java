/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.harness
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.harness;

import java.util.Vector;
import java.util.StringTokenizer;

/**
  <p>This class is for whatever java is in the current classpath

  @author ames
 */

public class currentjvm extends jvm {

	public String getName() {return "currentjvm";}
    public currentjvm(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
    long ss, long oss, long ms, long mx, String classpath, String prof,
    boolean verify, boolean noverify, boolean nojit, Vector D) {
        super(noasyncgc,verbosegc,noclassgc,ss,oss,ms,mx,classpath,prof,
		verify,noverify,nojit,D);
    }
    // more typical use:
    public currentjvm(String classpath, Vector D) {
        super(classpath,D);
    }
    // more typical use:
    public currentjvm(long ms, long mx, String classpath, Vector D) {
        super(ms,mx,classpath,D);
    }
    // actual use
    public currentjvm() { }

    // return the command line to invoke this VM.  The caller then adds
    // the class and program arguments.
    public Vector getCommandLine() 
    {
        StringBuffer sb = new StringBuffer();
        Vector v = super.getCommandLine();
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
        if (noasyncgc) sb.append(" -noasyncgc");
        if (verbosegc) sb.append(" -verbosegc");
        if (noclassgc) sb.append(" -noclassgc");
        if (ss>=0) {
          sb.append(" -ss");
          sb.append(ss);
        }
        if (oss>=0) {
          sb.append(" -oss");
          sb.append(oss);
        }
        if (ms>=0) {
          sb.append(" -ms");
          sb.append(ms);
        }
        if (mx>=0) {
          sb.append(" -mx");
          sb.append(mx);
        }
        if (classpath!=null) {
          sb.append(" -classpath ");
          sb.append(classpath);
        }
        if (prof!=null) {
          sb.append(" -prof:");
          sb.append(prof);
        }
        if (verify) sb.append(" -verify");
        if (noverify) sb.append(" -noverify");
        if (nojit) sb.append(" -nojit");
        if (D!=null)
          for (int i=0; i<D.size();i++) {
	        sb.append(" -D");
	        sb.append((String)(D.elementAt(i)));
          }
    }

	public String getDintro() { return "-D"; }
}
