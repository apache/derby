/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedClass;

/**
 * The Statement interface is an extension of exec prepared statement
 * that has some stored prepared specifics.
 *
 * @author jamie
 */
public interface StorablePreparedStatement extends ExecPreparedStatement
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
	 * Load up the class from the saved bytes.
	 *
	 *
	 * @exception StandardException on error
	 */
	public void loadGeneratedClass()
		throws StandardException;
}	
