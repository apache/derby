/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

/**
 *
 * In the family of activation support classes,
 * this one provides an activation with a cursor name.
 *
 * @author ames
 */
public abstract class CursorActivation 
	extends BaseActivation
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	/**
	 * remember the cursor name
	 */
	public void	setCursorName(String cursorName) 
	{
		if (!isClosed())
			super.setCursorName(cursorName);
	}

	/**
	 * @see org.apache.derby.iapi.sql.Activation#isCursorActivation
	 */
	public boolean isCursorActivation()
	{
		return true;
	}
}
