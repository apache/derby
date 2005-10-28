/*

   Derby - Class org.apache.derby.impl.sql.GenericLanguageFactory

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.io.FormatIdUtil;

import java.util.Properties;

/**
	The LanguageFactory provides system-wide services that
	are available on the Database API.

	@author ames
 */
public class GenericLanguageFactory implements LanguageFactory, ModuleControl
{

	private GenericParameterValueSet emptySet;

	public GenericLanguageFactory() { }

	/*
		ModuleControl interface
	 */

	/**
	 * Start-up method for this instance of the language factory.
	 * This service is expected to be started and accessed relative 
	 * to a database.
	 *
	 * @param startParams	The start-up parameters (ignored in this case)

       @exception StandardException Thrown if module cannot be booted.
	 *
	 */
	public void boot(boolean create, Properties startParams) throws StandardException 
	{		
		LanguageConnectionFactory lcf = (LanguageConnectionFactory)  Monitor.findServiceModule(this, LanguageConnectionFactory.MODULE);
		PropertyFactory pf = lcf.getPropertyFactory();
		if (pf != null)
			pf.addPropertySetNotification(new LanguageDbPropertySetter());

		emptySet = new GenericParameterValueSet(null, 0, false);
	}

	/**
	 * Stop this module.  In this case, nothing needs to be done.
	 *
	 * @return	Nothing
	 */

	public void stop() {
	}

	/* LanguageFactory methods */

	/**
	 * Factory method for getting a ParameterValueSet
	 *
	 * @see LanguageFactory#newParameterValueSet
	 */
	public ParameterValueSet newParameterValueSet(ClassInspector ci, int numParms, boolean hasReturnParam)
	{
		if (numParms == 0)
			return emptySet;

		return new GenericParameterValueSet(ci, numParms, hasReturnParam);
	}

	/**
	 * Get a new result description from the input result
	 * description.  Picks only the columns in the column
	 * array from the inputResultDescription.
	 *
 	 * @param inputResultDescription  the input rd
	 * @param theCols array of ints, non null
	 *
	 * @return ResultDescription the rd
	 */
	public ResultDescription getResultDescription
	(
		ResultDescription	inputResultDescription,
		int[]				theCols
	)
	{
		return new GenericResultDescription(inputResultDescription, theCols);
	} 

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
	)
	{
		return new GenericResultDescription(cols, type);
	}
 
	/*
	** REMIND: we will need a row and column factory
	** when we make putResultSets available for users'
	** server-side JDBC methods.
	*/
}
