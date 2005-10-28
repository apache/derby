/*

   Derby - Class org.apache.derby.impl.sql.execute.DropStatisticsConstantAction

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;


import org.apache.derby.catalog.UUID;

/**
 * this class drops all statistics for a particular table or index.
 *
 * @author manish.
 */

class DropStatisticsConstantAction extends DDLConstantAction
{
	private final String objectName;
	private final boolean forTable;
	private final SchemaDescriptor sd;
	private final String fullTableName;

	DropStatisticsConstantAction(SchemaDescriptor sd,
										String fullTableName,
										String objectName,
										boolean forTable)
	{
		this.objectName = objectName;
		this.sd = sd;
		this.forTable = forTable;
		this.fullTableName = fullTableName;
	}
	
	public void executeConstantAction(Activation activation)
		throws StandardException
	{
		TableDescriptor td;
		ConglomerateDescriptor cd = null;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();


		dd.startWriting(lcc);

		if (forTable)
		{
			td = dd.getTableDescriptor(objectName, sd);
		}
		
		else
		{
			cd = dd.getConglomerateDescriptor(objectName,
											 sd, false);
			td = dd.getTableDescriptor(cd.getTableID());
		}

		/* invalidate all SPS's on the table-- bad plan on SPS, so user drops
		 * statistics and would want SPS's invalidated so that recompile would
		 * give good plans; thats the theory anyways....
		 */
		dm.invalidateFor(td, DependencyManager.DROP_STATISTICS, lcc);

		dd.dropStatisticsDescriptors(td.getUUID(), ((cd != null) ? cd.getUUID() :
									 null), tc);
	}
	
	public String toString()
	{
		return "DROP STATISTICS FOR " + ((forTable) ? "table " : "index ") +
			fullTableName;
	}
}

