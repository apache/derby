/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.types.BooleanDataValue;

import org.apache.derby.iapi.error.StandardException;

/**
 * 	A TupleFilter is used to qualify rows from a tuple stream.
 *
 * @author Rick
 */
public interface TupleFilter
{
	/**
	  *	Initialize a Filter with a vector of parameters.
	  *
	  *	@param	parameters	An ExecRow of parameter values
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	void	init( ExecRow parameters )
				throws StandardException;

	/**
	  *	Pump a row through the Filter.
	  *
	  *	@param	row		Column values to plug into restriction
	  *
	  *	@return	True if the row qualifies. False otherwise.
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	BooleanDataValue	execute( ExecRow currentRow )
				throws StandardException;
}
