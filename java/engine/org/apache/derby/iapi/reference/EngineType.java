/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.reference
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.reference;


public interface EngineType
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	// Cloudscape engine types

	int			STANDALONE_DB			=	0x00000002;	
	int         STORELESS_ENGINE        =   0x00000080;

	int NONE = STANDALONE_DB;

	String PROPERTY = "derby.engineType";

}
