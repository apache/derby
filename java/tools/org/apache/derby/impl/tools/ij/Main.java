/*

   Derby - Class org.apache.derby.impl.tools.ij.Main

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.ij;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derby.iapi.tools.i18n.LocalizedInput;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Reader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.*;

/**
 * This is the controller for ij. It uses two parsers:
 * one to grab the next statement, and another to
 * see if it is an ij command, and if so execute it.
 * If it is not an ij command, it is treated as a JSQL
 * statement and executed against the current connection.
 * ijParser controls the current connection, and so contains
 * all of the state information for executing JSQL statements.
 * <p>
 * This was written to facilitate a test harness for language
 * functionality tests.
 *
 * @author ames
 *
 */
public class Main {
	public LocalizedOutput out;
	public utilMain utilInstance;
	public Class langUtilClass;



	/**
	 * ij can be used directly on a shell command line through
	 * its main program.
	 * @param args allows 1 file name to be specified, from which
	 *    input will be read; if not specified, stdin is used.
	 */
	public static void main(String[] args)	
		throws IOException 
	{
		mainCore(args, new Main(true));
	}

	public static void mainCore(String[] args, Main main)
		throws IOException 
	{
		LocalizedInput in = null;
		InputStream in1 = null;
		Main me;
		String file;
		String inputResourceName;
		boolean gotProp;

		String tmpUnicode = null;
		Properties connAttributeDefaults = null;

		// load the property file if specified
		gotProp = util.getPropertyArg(args);

		// get the default connection attributes
		connAttributeDefaults = util.getConnAttributeArg(args);
		// adjust the application in accordance with derby.ui.locale and derby.ui.codeset
    main.initAppUI();

		LocalizedResource langUtil = LocalizedResource.getInstance();
        
		LocalizedOutput out = langUtil.getNewOutput(System.out);

		file = util.getFileArg(args);
		inputResourceName = util.getInputResourceNameArg(args);
		if (util.invalidArgs(args, gotProp, file, inputResourceName)) {
			util.Usage(out);
      		return;
		}
		else if (inputResourceName != null) {
			in = langUtil.getNewInput(util.getResourceAsStream(inputResourceName));
			if (in == null) {
				out.println(langUtil.getTextMessage("IJ_IjErroResoNo",inputResourceName));
				return;
			}
		} else if (file == null) {
			in = langUtil.getNewInput(System.in);
            out.flush();
    	} else {
      		try {
				in1 = new FileInputStream(file);
				if (in1 != null) {
                    in1 = new BufferedInputStream(in1, utilMain.BUFFEREDFILESIZE);
					in = langUtil.getNewInput(in1);
                }
      		} catch (FileNotFoundException e) {
				if (Boolean.getBoolean("ij.searchClassPath")) {
					in = langUtil.getNewInput(util.getResourceAsStream(file));
                }
				if (in == null) {
				  out.println(langUtil.getTextMessage("IJ_IjErroFileNo",file));
        		  return;
				}
      		}
    	}

		// set initial Unicode Escape Mode
		tmpUnicode = util.getSystemProperty("ij.unicodeEscape");
		if ((tmpUnicode != null) && tmpUnicode.toUpperCase(Locale.ENGLISH).equals("ON")) {
			LocalizedResource.setUnicodeEscape(true);
		} 
		String outFile = util.getSystemProperty("ij.outfile");
		if (outFile != null && outFile.length()>0) {
			LocalizedOutput oldOut = out;
			try {
				out = langUtil.getNewOutput(new FileOutputStream(outFile));
			}
			catch (IOException ioe) {
				oldOut.println(langUtil.getTextMessage("IJ_IjErroUnabTo",outFile));
			}
		}

		// the old property name is deprecated...
		String maxDisplayWidth = util.getSystemProperty("maximumDisplayWidth");
		if (maxDisplayWidth==null) 
			maxDisplayWidth = util.getSystemProperty("ij.maximumDisplayWidth");
		if (maxDisplayWidth != null && maxDisplayWidth.length() > 0) {
			try {
				int maxWidth = Integer.parseInt(maxDisplayWidth);
				JDBCDisplayUtil.setMaxDisplayWidth(maxWidth);
			}
			catch (NumberFormatException nfe) {
				out.println(langUtil.getTextMessage("IJ_IjErroMaxiVa", maxDisplayWidth));
			}
		}

		/* Use the main parameter to get to
		 * a new Main that we can use.  
		 * (We can't do the work in Main(out)
		 * until after we do all of the work above
		 * us in this method.
		 */
		me = main.getMain(out);

		/* Let the processing begin! */
		me.go(in, out, connAttributeDefaults);
		in.close(); out.close();
	}

	/**
	 * Get the right Main (according to 
	 * the JDBC version.
	 *
	 * @return	The right main (according to the JDBC version).
	 */
	public Main getMain(LocalizedOutput out)
	{
		return new Main(out);
	}

	/**
	 * Get the right utilMain (according to 
	 * the JDBC version.
	 *
	 * @return	The right utilMain (according to the JDBC version).
	 */
	public utilMain getutilMain(int numConnections, LocalizedOutput out)
	{
		return new utilMain(numConnections, out);
	}

	/**
		Give a shortcut to go on the utilInstance so
		we don't expose utilMain.
	 */
	public void go(LocalizedInput in, LocalizedOutput out , 
				   Properties connAttributeDefaults)
	{
		LocalizedInput[] inA = { in } ;
		utilInstance.go(inA, out,connAttributeDefaults);
	}

	public void go(InputStream in, PrintStream out, 
				   Properties connAttributeDefaults)
	{
    initAppUI();
    	LocalizedResource langUtil = LocalizedResource.getInstance();
		go(langUtil.getNewInput(in), langUtil.getNewOutput(out),
			   connAttributeDefaults);
	}

	/**
	 * create an ij tool waiting to be given input and output streams.
	 */
	public Main() {
		this(null);
	}

	public Main(LocalizedOutput out) {
		if (out!=null) {
			this.out = out;
		} else {
	        this.out = LocalizedResource.getInstance().getNewOutput(System.out);
		}
		utilInstance = getutilMain(1, this.out);
	}

	/**
	 * This constructor is only used so that we 
	 * can get to the right Main based on the
	 * JDBC version.  We don't do any work in
	 * this constructor and we only use this
	 * object to get to the right Main via
	 * getMain().
	 */
	public Main(boolean trash)
	{
	}
  public void initAppUI(){
    //To fix a problem in the AppUI implementation, a reference to the AppUI class is
    //maintained by this tool.  Without this reference, it is possible for the
    //AppUI class to be garbage collected and the initialization values lost.
    //langUtilClass = LocalizedResource.class;

		// adjust the application in accordance with derby.ui.locale and derby.ui.codeset
	LocalizedResource.getInstance();	
  }
}
