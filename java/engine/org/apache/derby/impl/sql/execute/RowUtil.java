/*

   Derby - Class org.apache.derby.impl.sql.execute.RowUtil

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;
 
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Vector;

/**
  Utility class manipulating rows.

  <P>Note: this class is public so it may be used by Replication execution
  code. It is basically not public.
  */
public class RowUtil
{

 	/**
	  Get an empty ExecRow.

	  @param columnCount the number of columns in the row.
	  @return the row.
	  */
	public static ExecRow getEmptyValueRow(int columnCount, LanguageConnectionContext lcc)
	{
		ExecutionContext	ec;

		ec = lcc.getExecutionContext();
		return ec.getExecutionFactory().getValueRow(columnCount);
	}

 	/**
	  Get an empty ExecIndexRow.

	  @param columnCount the number of columns in the row.
	  @param cm			 Current ContextManager
	  @return the row.
	  */
	public static ExecIndexRow getEmptyIndexRow(int columnCount, ContextManager cm)
	{
		ExecutionContext	ec;

		ec = (ExecutionContext)
				cm.getContext(ExecutionContext.CONTEXT_ID);
		return ec.getExecutionFactory().getIndexableRow(columnCount);
	}

	/**
	  Clone an ExecRow's columns and place the coloned columns in another
	  ExecRow.

	  @param to Place the cloned columns here.
	  @param from Get the columns to clone here.
	  @param count Clone this number of columns.
	  */
	public static void copyCloneColumns(ExecRow to, ExecRow from, int count)
	{
		for (int ix = 1; ix <= count; ix++)
		{
			to.setColumn(ix,from.cloneColumn(ix));
		}
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.

	  @param to Place the column references here.
	  @param from Get the column references from here.
	  */
	public static void copyRefColumns(ExecRow to, ExecRow from)
	{
		Object[] src = from.getRowArray();
		Object[] dst = to.getRowArray();
		System.arraycopy(src, 0, dst, 0, src.length);
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.

	  @param to Place the column references here.
	  @param from Get the column references from here.
	  @param count Copy this number of column references.
	  */
	public static void copyRefColumns(ExecRow to, ExecRow from, int count)
		throws StandardException
	{
		copyRefColumns(to, 0, from, 0, count);
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.

	  @param to Place the column references here.
	  @param from Get the column references from here.
	  @param start The 0 based index of the first column to copy. 
	  @param count Copy this number of column references.
	  */
	public static void copyRefColumns(ExecRow to, ExecRow from,
									  int start, int count)
									  throws StandardException
	{
		copyRefColumns(to, 0, from, start, count);
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.
	  @param to Place the column references here.
	  @param toStart The 0-based index of the first column to replace.
	  @param from Get the column references from here.
	  @param fromStart The 0 based index of the first column to copy. 
	  @param count Copy this number of column references.
	  */
	public static void copyRefColumns(ExecRow to, int toStart, ExecRow from,
									  int fromStart, int count) throws StandardException {
		for (int i = 1; i <= count; i++)
		{
			// Uhhh, why doesn't this to an ArrayCopy????
			to.setColumn(i+toStart, from.getColumn(i+fromStart));
		}
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.

	  @param to Place the column references here.
	  @param from Get the column references from here.
	  @param positions	array of 1-based column ids to copy from "from" to "to"
	  */
	public static void copyRefColumns(ExecRow to, ExecRow from, int[] positions)
		throws StandardException
	{
		if ( positions == null ) { return; }

		int		count = positions.length;
		for (int ix = 0; ix < count; ix++)
		{ to.setColumn( ix + 1, from.getColumn( positions[ix] ) ); }
	}

	/**
	  Copy references for an ExecRow's columns to another ExecRow.
	  For copying from a compact array to a reconstituted array.
	  E.g. if positions = {2, 4}, and from = {666, 777} then
	  to => {null, 666, null, 777}.  Will only go as far as to.getArray().length.

	  @param to Place the column references here.  Sparse array
	  @param from Get the column references from here. Compact array
	  @param positions	array of 1-based column ids to copy from "from" to "to"
	  */
	public static void copyRefColumns(ExecRow to, ExecRow from, FormatableBitSet positions)
		throws StandardException
	{
		if (positions == null) 
		{ 
			return; 
		}

		int	max = to.getRowArray().length;
		int toCount = 1;
		int fromCount = 1;
		for (;toCount <= max; toCount++)
		{
			if (positions.get(toCount))
			{
				to.setColumn(toCount, from.getColumn(fromCount)); 
				fromCount++;
			}
		}
	}

	/**
	  Empty columns -- i.e. make them refer to a java null.

	  <P>This is useful to remove dangling references to a column.

	  @param setMe Set columns in this storable to be empty.
	  @param count Number of columns to empty.
	  */
	public static void copyRefColumns(ExecRow setMe)
		throws StandardException
	{
		for (int ix = 1; ix <= setMe.nColumns(); ix++)
		{
			setMe.setColumn(ix,null);
		}
	}

	/**
	 * toString
	 *
	 * @param row 			the row
	 *
	 * @return the string
	 */
	public static String toString(ExecRow row)
	{
		if (SanityManager.DEBUG)
		{
			return (row == null) ? "null" : toString(row.getRowArray());
		}
		else
		{
			return "";
		}
	}
		
	/**
	 * toString
	 *
	 * @param objs 			the row array
	 *
	 * @return the string
	 */
	public static String toString(Object[] objs)
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer strbuf = new StringBuffer();

			if (objs == null) 
				return "null";

			strbuf.append("(");
			for (int i = 0; i < objs.length; i++)
			{
				if (i > 0)
				{
					strbuf.append(",");
				}
				strbuf.append(objs[i]);
			}
			strbuf.append(")");
			return strbuf.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * toString
	 * 
	 * @param row 			the row
	 * @param startPoint 	0 based start point in row array, inclusive
	 * @param endPoint		0 based end point in row array, inclusive
	 * 
	 * @return the string
	 */
	public static String toString(ExecRow row, int startPoint, int endPoint)
	{
		return toString(row.getRowArray(), startPoint, endPoint);
	}

	/**
	 * toString
	 * 
	 * @param objs 			the row array
	 * @param startPoint 	0 based start point in row array, inclusive
	 * @param endPoint		0 based end point in row array, inclusive
	 * 
	 * @return the string
	 */
	public static String toString(Object[] objs, int startPoint, int endPoint)
	{
		StringBuffer strbuf = new StringBuffer();

		if (SanityManager.DEBUG)
		{
			if (endPoint >= objs.length)
			{
				SanityManager.THROWASSERT("endPoint "+endPoint+" is too high,"+
					" array only has "+objs.length+" elements");
			}
		}
		strbuf.append("(");
		for (int i = startPoint; i <= endPoint; i++)
		{
			if (i > 0)
			{
				strbuf.append(",");
			}
			strbuf.append(objs[i]);
		}
		strbuf.append(")");
		return strbuf.toString();
	}


	/**
	 * toString
	 * 
	 * @param row 			the row
	 * @param positions 	1 based array of positions
	 * 
	 * @return the string
	 */
	public static String toString(ExecRow row, int[] positions)
	{
		return toString(row.getRowArray(), positions);
	}

	/**
	 * toString
	 * 
	 * @param objs 			the row array
	 * @param positions 	1 based array of positions
	 * 
	 * @return the string
	 */
	public static String toString(Object[] objs, int[] positions)
	{
		if (positions == null)
		{
			return (String) null;
		}

		StringBuffer strbuf = new StringBuffer();

		strbuf.append("(");
		for (int i = 0; i < positions.length; i++)
		{

			if (i > 0)
			{
				strbuf.append(",");
			}
			strbuf.append(objs[positions[i] - 1]);
		}
		strbuf.append(")");
		return strbuf.toString();
	}

	/**
	 * intArrayToString
	 *
	 * @param objs 			the int array
	 *
	 * @return the string
	 */
	public static String intArrayToString(int[] colMap)
	{
		StringBuffer strbuf = new StringBuffer();

		strbuf.append("(");
		for (int i = 0; i < colMap.length; i++)
		{
			if (i > 0)
			{
				strbuf.append(",");
			}
			strbuf.append(colMap[i]);
		}
		strbuf.append(")");
		return strbuf.toString();
	}

	public static boolean inAscendingOrder(int[] colMap)
	{
		if (colMap != null)
		{
			int lastCol = -1;
			for (int i = 0; i < colMap.length; i++)
			{
				if (lastCol > colMap[i])
				{	
					return false;
				}
				lastCol = colMap[i];
			}
		}
		return true;
	}	
	/**
	 * Shift a FormatableBitSet N bits toward the zero end.
	 * e.g. shift({2,4}) -> {1,3}.
	 *
	 * @param bitSet the bit set
	 * @param n	the number of bits to shift
	 *
	 * @return a new FormatableBitSet with the shifted result
	 */
	public static FormatableBitSet shift(FormatableBitSet bitSet, int n)
	{
		FormatableBitSet out = null;
		if (bitSet != null)
		{
			int size = bitSet.size();
 			out = new FormatableBitSet(size);
			for (int i = n; i < size; i++)
			{
				if (bitSet.get(i))
				{
					out.set(i-n);
				}
			}	
		}
		return out;
	}
}
