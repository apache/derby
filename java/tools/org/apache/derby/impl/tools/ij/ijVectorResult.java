/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

import java.util.Vector;
import java.sql.SQLWarning;

/**
 * This is an impl for a simple Vector of strings.
 *
 * @author ames
 */
class ijVectorResult extends ijResultImpl {

	Vector vec;
	SQLWarning warns;

	ijVectorResult(Vector v, SQLWarning w) {
		vec = v;
		warns = w;
	}

	public boolean isVector() { return true; }

	public Vector getVector() { return vec; }

	public SQLWarning getSQLWarnings() { return warns; }
	public void clearSQLWarnings() { warns = null; }
}
