/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.db
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.services.context.Context;


/**
  A context for a database.
  */
public interface DatabaseContext extends Context {
	public static final String CONTEXT_ID = "Database";
	public Database getDatabase();
}
