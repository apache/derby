/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.store.access.ColumnOrdering;

import org.apache.derby.impl.sql.execute.IndexColumnOrder;

import java.util.Hashtable;

/**
 * List of OrderedColumns
 *
 * @author Jamie
 */
public abstract class OrderedColumnList extends QueryTreeNodeVector
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Get an array of ColumnOrderings to pass to the store
	 */
	public IndexColumnOrder[] getColumnOrdering()
	{
		IndexColumnOrder[] ordering;
		int numCols = size();
		int actualCols;

		ordering = new IndexColumnOrder[numCols];

		/*
			order by is fun, in that we need to ensure
			there are no duplicates in the list.  later copies
			of an earlier entry are considered purely redundant,
			they won't affect the result, so we can drop them.
			We don't know how many columns are in the source,
			so we use a hashtable for lookup of the positions
		*/
		Hashtable hashColumns = new Hashtable();

		actualCols = 0;

		for (int i = 0; i < numCols; i++)
		{
			OrderedColumn oc = (OrderedColumn) elementAt(i);

			// order by (lang) positions are 1-based,
			// order items (store) are 0-based.
			int position = oc.getColumnPosition() - 1;

			Integer posInt = new Integer(position);

			if (! hashColumns.containsKey(posInt))
			{
				ordering[i] = new IndexColumnOrder(position,
												oc.isAscending());
				actualCols++;
				hashColumns.put(posInt, posInt);
			}
		}

		/*
			If there were duplicates removed, we need
			to shrink the array down to what we used.
		*/
		if (actualCols < numCols)
		{
			IndexColumnOrder[] newOrdering = new IndexColumnOrder[actualCols];
			System.arraycopy(ordering, 0, newOrdering, 0, actualCols);
			ordering = newOrdering;
		}

		return ordering;
	}
}
