/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import java.io.IOException;
import java.sql.SQLException;
/**
 * Used for fatal IJ exceptions
 */

public class ijFatalException extends RuntimeException {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private final static String FatalException = LocalizedResource.getMessage("IJ_FataExceTerm");
	private SQLException e;

	public ijFatalException() 
	{
		super(FatalException);
		e = null;
	}

	public ijFatalException(SQLException e) 
	{
		super(FatalException); 
		this.e = e;
	}

	public String getSQLState()
	{
		return e.getSQLState();
	}
	
	public String toString()
	{
		return LocalizedResource.getMessage("IJ_Fata01",e.getSQLState(),e.getMessage());
	}
}
