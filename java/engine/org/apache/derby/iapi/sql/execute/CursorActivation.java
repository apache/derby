/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.sql.Activation;

/**
 * CursorActivation includes an additional method used on cursors.
 *
 * @author ames
 */
public interface CursorActivation extends Activation {

	/**
	 * Returns the target result set for this activation,
	 * so that the current base row can be determined.
	 *
	 * @return the target ResultSet of this activation.
	 */
	CursorResultSet getTargetResultSet();

	/**
	 * Returns the cursor result set for this activation,
	 * so that the current row can be re-qualified, and
	 * so that the current row location can be determined.
	 */
	CursorResultSet getCursorResultSet();
}
