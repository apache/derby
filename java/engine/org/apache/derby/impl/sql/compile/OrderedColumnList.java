/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderedColumnList

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
