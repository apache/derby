/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.lang.ClassNotFoundException;

import java.io.IOException; 

/**
  Formatable for holding SQL data (which may be null).
  @see Formatable
 */
public interface Storable
extends Formatable
{
	/**
		IBM Copyright &copy notice.
	*/
 
    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	  Return whether the value is null or not.
	  
	  @return true if the value is null and false otherwise.
	**/
	public boolean isNull();

	/**
	  Restore this object to its (SQL)null value.
	**/
	public void restoreToNull();
}
