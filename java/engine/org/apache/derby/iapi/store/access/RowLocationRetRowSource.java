/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

/**

  A RowLocationRetRowSource is the mechanism for iterating over a set of rows,
  loading those rows into a conglomerate, and returning the RowLocation of the
  inserted rows. 

  @see RowSource

*/ 
public interface RowLocationRetRowSource extends RowSource 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
		needsRowLocation returns true iff this the row source expects the
		drainer of the row source to call rowLocation after getting a row from
		getNextRowFromRowSource.

		@return true iff this row source expects some row location to be
		returned 
		@see #rowLocation
	 */
	boolean needsRowLocation();

	/**
		rowLocation is a callback for the drainer of the row source to return
		the rowLocation of the current row, i.e, the row that is being returned
		by getNextRowFromRowSource.  This interface is for the purpose of
		loading a base table with index.  In that case, the indices can be
		built at the same time the base table is laid down once the row
		location of the base row is known.  This is an example pseudo code on
		how this call is expected to be used:
		
		<BR><pre>
		boolean needsRL = rowSource.needsRowLocation();
		DataValueDescriptor[] row;
		while((row = rowSource.getNextRowFromRowSource()) != null)
		{
			RowLocation rl = heapConglomerate.insertRow(row);
			if (needsRL)
				rowSource.rowLocation(rl);
		}
		</pre><BR>

		NeedsRowLocation and rowLocation will ONLY be called by a drainer of
		the row source which CAN return a row location.  Drainer of row source
		which cannot return rowLocation will guarentee to not call either
		callbacks. Conversely, if NeedsRowLocation is called and it returns
		true, then for every row return by getNextRowFromRowSource, a
		rowLocation callback must also be issued with the row location of the
		row.  Implementor of both the source and the drain of the row source
		must be aware of this protocol.

		<BR>
		The RowLocation object is own by the caller of rowLocation, in other
		words, the drainer of the RowSource.  This is so that we don't need to
		new a row location for every row.  If the Row Source wants to keep the
		row location, it needs to clone it (RowLocation is a ClonableObject).
		@exception StandardException on error
	 */
	void rowLocation(RowLocation rl) throws StandardException;
}
