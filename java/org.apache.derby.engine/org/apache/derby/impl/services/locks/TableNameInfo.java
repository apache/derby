/*

   Derby - Class org.apache.derby.impl.services.locks.TableNameInfo

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.locks;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.store.access.TransactionController;

import java.util.Hashtable;
import org.apache.derby.catalog.UUID;

@SuppressWarnings("UseOfObsoleteCollectionType")
public class TableNameInfo {

	// things to look up table name etc
	private DataDictionary dd;

    // conglomId -> conglomerateDescriptor
    private Hashtable<Long, ConglomerateDescriptor> ddCache;

    // tableID UUID -> table descriptor
    private Hashtable<UUID, TableDescriptor> tdCache;

    private Hashtable<Long,TableDescriptor> tableCache;     // conglomId -> table descriptor
	private Hashtable<Long,String> indexCache;		// conglomId -> indexname

	public TableNameInfo(LanguageConnectionContext lcc, boolean andIndex)
		throws StandardException {

		tableCache = new Hashtable<Long,TableDescriptor>(31);
		if (andIndex)
			indexCache = new Hashtable<Long,String>(13);

		TransactionController tc = lcc.getTransactionExecute();

		dd = lcc.getDataDictionary();
		ddCache = dd.hashAllConglomerateDescriptorsByNumber(tc);
		tdCache = dd.hashAllTableDescriptorsByTableId(tc);
	}


	public String getTableName(Long conglomId) {
		if (conglomId == null)
			return "?";

		// see if we have already seen this conglomerate
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		TableDescriptor td = tableCache.get(conglomId);
		if (td == null)
		{
			// first time we see this conglomerate, get it from the
			// ddCache 
            ConglomerateDescriptor cd = ddCache.get(conglomId);
//IC see: https://issues.apache.org/jira/browse/DERBY-673

            if (cd != null)
            {
                // conglomerateDescriptor is not null, this table is known
                // to the data dictionary

                td = tdCache.get(cd.getTableID());
            }

			if ((cd == null) || (td == null))
			{
				String name;

				// this table is not know to the data dictionary.  This
				// can be caused by one of two reasons:  
				// 1. the table has just been dropped
				// 2. the table is an internal one that lives below
				// 		the data dictionary
				if (conglomId.longValue() > 20)
				{
					// table probably dropped!  
					name = "*** TRANSIENT_" + conglomId;
				}
				else
				{
					// I am hoping here that we won't create more than
					// 20 tables before starting the data dictionary!

					// one of the internal tables -- HACK!!
					switch (conglomId.intValue())
					{
					case 0: 
						name = "*** INVALID CONGLOMERATE ***";
						break;

					case 1: 
						name = "ConglomerateDirectory";
						break;

					case 2: 
						name = "PropertyConglomerate";
						break;

					default:
						name = "*** INTERNAL TABLE " + conglomId;
						break;
					}
				}

				return name;
			}

            tableCache.put(conglomId, td);

			if ((indexCache != null) && cd.isIndex())
				indexCache.put(conglomId, cd.getConglomerateName());
		}

		return td.getName();
	}

	public String getTableType(Long conglomId) {
		if (conglomId == null)
			return "?";

		String type;

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		TableDescriptor td = tableCache.get(conglomId);
		if (td != null)
		{
			switch(td.getTableType())
			{
			case TableDescriptor.BASE_TABLE_TYPE:
				type = "T";
				break;

			case TableDescriptor.SYSTEM_TABLE_TYPE:
				type = "S";
				break;

			default: 
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("Illegal table type " +
						  td.getName() + " " + td.getTableType());
				type = "?";
				break;
			}
		} else if (conglomId.longValue() > 20)
		{
			type = "T";
		} else {
			type = "S";
		}

		return type;
	}

	public String getIndexName(Long conglomId) {
		if (conglomId == null)
			return "?";
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		return indexCache.get(conglomId);
	}
}
