/*

   Derby - Class org.apache.derby.impl.tools.ij.Main14

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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
