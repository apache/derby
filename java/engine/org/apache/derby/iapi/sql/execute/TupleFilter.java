/*

   Derby - Class org.apache.derby.iapi.sql.execute.TupleFilter

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
