/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.tools
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.tools;

import org.apache.derby.iapi.services.info.JVMInfo;

import org.apache.derby.impl.tools.ij.Main;

import java.io.IOException;

/**
	
	ij is Cloudscape's interactive JDBC scripting tool.
	It is a simple utility for running scripts against a Cloudscape database.
	You can also use it interactively to run ad hoc queries.
	ij provides several commands for ease in accessing a variety of JDBC features.
	<P>

	To run from the command line enter the following:
	<p>
	java [options] org.apache.derby.tools.ij [arguments]
	<P>
	ij is can also be used with any database server that supports a JDBC driver.
*/
public class ij {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

  /**
  	@exception IOException thrown if cannot access input or output files.
   */
  static public void main(String[] args) throws IOException {

	  /* We decide which verion of ij (2.0 or 4.0) to
	   * load based on the same criteria that the JDBC driver
	   * uses.
	   */
	  if (JVMInfo.JDK_ID == 2)
	  {
		  Main.main(args);
	  }
	  else
	  {
		  org.apache.derby.impl.tools.ij.Main14.main(args);
	  }
  }

  private ij() { // no instances allowed
  }
  
  public static String getArg(String param, String[] args)
  {
	  return org.apache.derby.impl.tools.ij.util.getArg(param, args);
  }

  public static void getPropertyArg(String[] args) throws IOException
  {
	  org.apache.derby.impl.tools.ij.util.getPropertyArg(args);
  }

  public static java.sql.Connection startJBMS()
	  throws java.sql.SQLException, IllegalAccessException, ClassNotFoundException, InstantiationException
  {			
		return org.apache.derby.impl.tools.ij.util.startJBMS();
  }
}
