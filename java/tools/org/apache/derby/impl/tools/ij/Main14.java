/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import org.apache.derby.tools.JDBCDisplayUtil;


import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
/**
 * This is the controller for the JDBC3.0 version
 * of ij.
 * <p>
 * This was written to facilitate a test harness for the
 * holding cursors over commit functionality in JDBC3.0.
 *
 */
public class Main14 extends Main
{
	/**
	 * ij can be used directly on a shell command line through
	 * its main program.
	 * @param args allows 1 file name to be specified, from which
	 *    input will be read; if not specified, stdin is used.
	 */
	public static void main(String[] args)	throws IOException 
	{
		Main.mainCore(args, new Main14(true));
	}

	/**
	 * create an ij tool waiting to be given input and output streams.
	 */
	public Main14() 
	{
		this(null);
	}

	public Main14(LocalizedOutput out) 
	{
		super(out);
	}

	/**
	 * This constructor is only used so that we 
	 * can get to the right Main based on the
	 * JDBC version.  We don't do any work in
	 * this constructor and we only use this
	 * object to get to the right Main via
	 * getMain().
	 */
	public Main14(boolean trash)
	{
		super(trash);
	}
	/**
	 * Get the right Main (according to 
	 * the JDBC version.
	 *
	 * @return	The right Main (according to the JDBC version).
	 */
	public Main getMain(LocalizedOutput out)
	{
		return new Main14(out);
	}

	/**
	 * Get the right utilMain (according to 
	 * the JDBC version.
	 *
	 * @return	The right utilMain (according to the JDBC version).
	 */
	public utilMain getutilMain(int numConnections, LocalizedOutput out)
	{
		return new utilMain14(numConnections, out);
	}

}
