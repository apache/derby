/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.RowLocation;


public interface RefDataValue extends DataValueDescriptor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
	 * Set the value of this RefDataValue.
	 *
	 * @param theValue	Contains the boolean value to set this RefDataValue
	 *					to.  Null means set this RefDataValue to null.
	 *
	 * @return	This RefDataValue
	 *
	 */
	public void setValue(RowLocation theValue);
}
