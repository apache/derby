/*

   Derby - Class org.apache.derbyTesting.functionTests.util.ConsistencyChecker

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

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * This class provides static methods for checking the consistency of database
 * objects like tables.
 */
public class ConsistencyChecker
{

	/**
	 * Run all of the consistency checkers which do not take parameters.
	 * Actually, just run the ones that "make sense" to run.  Today,
	 * that is:
	 *		countOpens()
	 *
	 * @return String		If an inconsistency is found, and if DEBUG is on, 
	 *						then a string will be returned with more info.  
	 *						If DEBUG is off, then a simple string will be 
	 *						returned stating whether or not there are open scans.
	 *
	 * @exception StandardException		Thrown on error
	 * @exception java.sql.SQLException		Thrown on error
	 */
	public static String runConsistencyChecker() throws StandardException, java.sql.SQLException
	{
		return countOpens() + countDependencies();
	}

	/**
	 * Check to make sure that there are no open conglomerates, scans or sorts.
	 *
	 * @return String		If an inconsistency is found, and if DEBUG is on, 
	 *						then a string will be returned with more info.  
	 *						If DEBUG is off, then a simple string will be 
	 *						returned stating whether or not there are open scans.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static String countOpens() throws StandardException
	{
		int						numOpens = 0;
		LanguageConnectionContext lcc;
		String					output = "No open scans, etc.\n";
		TransactionController	tc;

		lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		tc = lcc.getTransactionExecute();

		numOpens = tc.countOpens(TransactionController.OPEN_TOTAL);

		if (numOpens > 0)
		{
            output = numOpens + " conglomerates/scans/sorts found open\n";

		}

		return output;
	}

	/**
	 * Check to make sure that there are no active dependencies (stored or
	 * in memory).
	 *
	 * @return String		If an inconsistency is found, and if DEBUG is on, 
	 *						then a string will be returned with more info.  
	 *						If DEBUG is off, then a simple string will be 
	 *						returned stating whether or not there are open scans.
	 *
	 * @exception StandardException		Thrown on error
	 * @exception java.sql.SQLException		Thrown on error
	 */
	public static String countDependencies() throws StandardException, java.sql.SQLException
	{
		int						numDependencies = 0;
		DataDictionary			dd;
		DataDictionaryContext	ddc;
		DependencyManager		dm;
		StringBuffer			debugBuf = new StringBuffer();

		ddc = (DataDictionaryContext)
				(ContextService.getContext(DataDictionaryContext.CONTEXT_ID));

		dd = ddc.getDataDictionary();
		dm = dd.getDependencyManager();

		numDependencies = dm.countDependencies();

		if (numDependencies > 0)
		{
            debugBuf.append(numDependencies + " dependencies found");
		}
		else
		{
			debugBuf.append("No outstanding dependencies.\n");
		}

		return debugBuf.toString();
	}
}
