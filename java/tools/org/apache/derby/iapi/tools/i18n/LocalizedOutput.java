/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.tools.i18n
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derby.iapi.tools.i18n;

import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

public class LocalizedOutput extends PrintWriter {
	/**
			IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;
	private OutputStream out;
	public LocalizedOutput(OutputStream o){
		super(new OutputStreamWriter(o), true);
		out = o;
	}
	public LocalizedOutput(OutputStream o, String enc) throws UnsupportedEncodingException {
		super(new OutputStreamWriter(o, enc), true);
		out = o;
	}
	public boolean isStandardOutput(){
		return (out == System.out);
	}
	public void close() {
		if (!isStandardOutput()) {
			super.close();
		}
	}
}
