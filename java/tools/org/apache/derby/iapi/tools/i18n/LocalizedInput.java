/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.tools.i18n
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derby.iapi.tools.i18n;

import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;

public class LocalizedInput extends InputStreamReader{
	/**
			IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;
	private InputStream in;
	public LocalizedInput(InputStream i){
		super(i);
		this.in = i;
	}
	public LocalizedInput(InputStream i, String encode) throws UnsupportedEncodingException{
		super(i,encode);
		this.in = i;
	}
	public boolean isStandardInput(){
		return (in == System.in);
	}
	public void close() throws IOException {
		if (!isStandardInput()) {
			super.close();
		}
	}

}
