/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.depend
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.UUID;

/**
	A dependency represents a reliance of the dependent on
	the provider for some information the dependent contains
	or uses.  In Language, the usual case is a prepared statement
	using information about a schema object in its executable form.
	It needs to be notified if the schema object changes, so that
	it can recompile against the new information.
 */
public interface Dependency { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
		return the provider's key for this dependency.
		@return the provider's key for this dependency
	 */
	UUID getProviderKey();

	/**
		return the provider for this dependency.
		@return the provider for this dependency
	 */
	Provider getProvider();

	/**
		return the dependent for this dependency.
		@return the dependent for this dependency
	 */
	Dependent getDependent();

}
