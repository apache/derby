/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

/**
 * This is a simple interface that is used by the
 * sorter for cloning input rows.  It defines
 * a method that can be used to clone a column.
 */
public interface CloneableObject
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Get a shallow copy of the object and return
	 * it.  This is used by the sorter to clone
	 * columns.  It should be cloning the column
	 * holder but not its value.  The only difference
	 * between this method and getClone is this one does
	 * not objectify a stream.
	 *
	 * @return new cloned column as an object
	 */
	public Object cloneObject();
}
