/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * An ordered column has position.   It is an
 * abstract class for group by and order by
 * columns.
 *
 * @author jamie
 */
public abstract class OrderedColumn extends QueryTreeNode 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	protected static final int UNMATCHEDPOSITION = -1;
	protected int	columnPosition = UNMATCHEDPOSITION;

	/**
	 * Indicate whether this column is ascending or not.
	 * By default assume that all ordered columns are
	 * necessarily ascending.  If this class is inherited
	 * by someone that can be desceneded, they are expected
	 * to override this method.
	 *
	 * @return true
	 */
	public boolean isAscending()
	{
		return true;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
	public String toString() 
	{
		if (SanityManager.DEBUG)
		{
			return "columnPosition: " + columnPosition + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Get the position of this column
	 *
	 * @return	The position of this column
	 */
	public int getColumnPosition() 
	{
		return columnPosition;
	}

	/**
	 * Set the position of this column
	 *
	 * @return	Nothing
	 */
	public void setColumnPosition(int columnPosition) 
	{
		this.columnPosition = columnPosition;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(columnPosition > 0,
				"Column position is " + columnPosition +
				". This is a problem since the code to generate " +
				" ordering columns assumes it to be one based -- i.e. "+
				" it subtracts one");

		}
	}
}
