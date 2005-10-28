/*

   Derby - Class org.apache.derby.iapi.sql.LanguageFactory

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassInspector;

/**
 * Factory interface for the Language.Interface protocol.
 * This is used via the Database API by users, and is presented
 * as a System Module (not a service module).  That could change,
 * but for now this is valid for any database. 
 *
 * @author Jeff Lichtman
 */
public interface LanguageFactory
{
	/**
		Used to locate this factory by the Monitor basic service.
		There needs to be a language factory per database.
	 */
	String MODULE = "org.apache.derby.iapi.sql.LanguageFactory";

	/**
	 * Get a ParameterValueSet
	 *
	 * @param numParms	The number of parameters in the
	 *			ParameterValueSet
	 * @param hasReturnParam	true if this parameter set
	 *			has a return parameter.  The return parameter
	 *			is always the 1st parameter in the list.  It
	 *			is due to a callableStatement like this: <i>
	 *			? = CALL myMethod()</i>
	 *
	 * @return	A new ParameterValueSet with the given number of parms
	 */
	ParameterValueSet newParameterValueSet(ClassInspector ci, int numParms, boolean hasReturnParam);

	/**
	 * Get a new result description from the input result
	 * description.  Picks only the columns in the column
	 * array from the inputResultDescription.
	 *
 	 * @param inputResultDescription the input rd
	 * @param theCols non null array of ints
	 *
	 * @return ResultDescription the rd
	 */
	public ResultDescription getResultDescription
	(
		ResultDescription	inputResultDescription,
		int[]				theCols
	);

	/**
	 * Get a new result description
	 *
 	 * @param cols an array of col descriptors
	 * @param type the statement type
	 *
	 * @return ResultDescription the rd
	 */
	public ResultDescription getResultDescription
	(
		ResultColumnDescriptor[]	cols,
		String						type
	);
}
