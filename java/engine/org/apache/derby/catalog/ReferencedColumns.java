/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;


/**
 *	
 *  Provides information about the columns that are referenced by a
 *  CHECK CONSTRAINT definition.
 *  
 *  It is used in the column SYS.SYSCHECKS.REFERENCEDCOLUMNSDESCRIPTOR.
 */
public interface ReferencedColumns
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Returns an array of 1-based column positions in the table that the
	 * check constraint is on.  
	 *
	 * @return	An array of ints representing the 1-based column positions
	 *			of the columns that are referenced in this check constraint.
	 */
	public int[]	getReferencedColumnPositions();
}
