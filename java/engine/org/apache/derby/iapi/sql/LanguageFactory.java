/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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
