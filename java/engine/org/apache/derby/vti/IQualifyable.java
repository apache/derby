/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.vti
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.vti;

import java.sql.SQLException;
import org.apache.derby.iapi.services.io.Storable;

public interface IQualifyable {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	// public boolean handleQualifier(int relOp, int 

	/**
		Called at runtime before each scan of the VTI.
		The passed in qualifiers are only valid for the single
		execution that follows.
	*/
	public void setQualifiers(VTIEnvironment vtiEnvironment, org.apache.derby.iapi.store.access.Qualifier[][] qualifiers)
		throws SQLException;
}
