/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.depend
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.depend;

import	org.apache.derby.catalog.Dependable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
	A dependent has the ability to know whether or not it
	is valid and to mark itself as valid or invalid.
	Marking itself as invalid usually means it cannot be used
	in the system until it is revalidated, but this is in no
	way enforced by this interface.
 */
public interface Dependent  extends Dependable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
		Check that all of the dependent's dependencies are valid.

		@return true if the dependent is currently valid
	 */
	boolean isValid();

	/**
		Prepare to mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param action	The action causing the invalidation
		@param p		the provider
		@param lcc		The LanguageConnectionContext

		@exception StandardException thrown if unable to make it invalid
	 */
	void prepareToInvalidate(Provider p, int action, 
							 LanguageConnectionContext lcc) 
		throws StandardException;

	/**
		Mark the dependent as invalid (due to at least one of
		its dependencies being invalid).

		@param	action	The action causing the invalidation
		@param lcc		The LanguageConnectionContext

		@exception StandardException thrown if unable to make it invalid
	 */
	void makeInvalid(int action,
					 LanguageConnectionContext lcc) 
			throws StandardException;

	/**
		Attempt to revalidate the dependent. For prepared statements,
		this could go through its dependencies and check that they
		are up to date; if not, it would recompile the statement.
		Any failure during this attempt should throw
		DependencyStatementException.unableToRevalidate().

		@param lcc		The LanguageConnectionContext

		@exception StandardException thrown if unable to make it valid
	 */
	void makeValid(LanguageConnectionContext lcc) 
		throws StandardException;
}
