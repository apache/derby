/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;


/**

  The column ordering interface defines a column that is to be
  ordered in a sort or index, and how it is to be ordered.  Column
  instances are compared by calling the compare(Orderable) method
  of Orderable.

**/

public interface ColumnOrdering
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	int getColumnId();
	boolean getIsAscending();
}

