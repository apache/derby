/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderedColumnList

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package	org.apache.derby.impl.sql.compile;

import java.util.HashSet;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;

/**
 * List of OrderedColumns
 *
 */
public abstract class OrderedColumnList<E extends OrderedColumn>
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    extends QueryTreeNodeVector<E>
{
    public OrderedColumnList(Class<E> eltClass,
            ContextManager cm) {
        super(eltClass, cm);
    }

	/**
	 * Get an array of ColumnOrderings to pass to the store
	 */
    IndexColumnOrder[] getColumnOrdering()
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
        HashSet<Integer> hashColumns = new HashSet<Integer>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		actualCols = 0;

		for (int i = 0; i < numCols; i++)
		{
            OrderedColumn oc = elementAt(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-673

			// order by (lang) positions are 1-based,
			// order items (store) are 0-based.
			int position = oc.getColumnPosition() - 1;

//IC see: https://issues.apache.org/jira/browse/DERBY-6885
			if (hashColumns.add(position))
			{
				ordering[i] = new IndexColumnOrder(position,
//IC see: https://issues.apache.org/jira/browse/DERBY-2887
												oc.isAscending(),
												oc.isNullsOrderedLow());
				actualCols++;
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
