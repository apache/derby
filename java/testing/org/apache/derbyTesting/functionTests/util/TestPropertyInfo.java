/*

   Derby - Class org.apache.derbyTesting.functionTests.util.TestPropertyInfo

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

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.db.PropertyInfo;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import java.util.Properties;

/**
 * This class extends PropertyInfo to provide support for viewing ALL
 * table/index properties, not just the user-visible ones.
 */
public class TestPropertyInfo
{

    /**
     * Get ALL the Properties associated with a given table, not just the
	 * customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @return Properties   The Properties associated with the specified table.
     *                      (An empty Properties is returned if the table does not exist.)
     * @exception java.sql.SQLException thrown on error
     */
    public static String getAllTableProperties(String schemaName, String tableName)
        throws java.sql.SQLException
	{
		Properties p =	TestPropertyInfo.getConglomerateProperties( schemaName, tableName, false );
		if (p == null)
			return null;

		return org.apache.derbyTesting.functionTests.util.PropertyUtil.sortProperties(p);
	}

/**
     * Get a specific property  associated with a given table, not just the
	 * customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @param key           The table property  to retrieve
	 * @return               Property value 
     * @exception java.sql.SQLException thrown on error
     */
	public static String getTableProperty(String schemaName, String tableName,
										  String key) throws java.sql.SQLException
	{
		return TestPropertyInfo.getConglomerateProperties( schemaName, tableName, false ).getProperty(key);
	}

    /**
     * Get ALL the Properties associated with a given index, not just the customer-visible ones.
     *
	 * @param schemaName    The name of the schema that the index is in.
	 * @param indexName     The name of the index.
	 * 
	 * @return Properties   The Properties associated with the specified index.
     *                      (An empty Properties is returned if the index does not exist.)
     * @exception java.sql.SQLException thrown on error
     */
    public static String getAllIndexProperties(String schemaName, String indexName)
        throws java.sql.SQLException
	{
		Properties p = TestPropertyInfo.getConglomerateProperties( schemaName, indexName, true );

		if (p == null)
			return null;

		return org.apache.derbyTesting.functionTests.util.PropertyUtil.sortProperties(p);
	}

	/**
	  Return the passed in Properties object with a property filtered out.
	  This is useful for filtering system depenent properties to make
	  test canons stable.
	  */
	public static Properties filter(Properties p, String filterMe)
	{
		p.remove(filterMe);
		return p;
	}

	private static Properties	getConglomerateProperties( String schemaName, String conglomerateName, boolean isIndex )
        throws java.sql.SQLException
	{
		ConglomerateController    cc;
		ConglomerateDescriptor    cd;
		DataDictionary            dd;
		Properties				  properties;
		SchemaDescriptor		  sd;
		TableDescriptor           td;
		TransactionController     tc;
		long					  conglomerateNumber;

        // find the language context.
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

        // Get the current transaction controller
        tc = lcc.getTransactionExecute();

		try {

		// find the DataDictionary
		dd = lcc.getDataDictionary();


		// get the SchemaDescriptor
		sd = dd.getSchemaDescriptor(schemaName, tc, true);
		if ( !isIndex)
		{
			// get the TableDescriptor for the table
			td = dd.getTableDescriptor(conglomerateName, sd);

			// Return an empty Properties if table does not exist or if it is for a view.
			if ((td == null) || td.getTableType() == TableDescriptor.VIEW_TYPE) { return new Properties(); }

			conglomerateNumber = td.getHeapConglomerateId();
		}
		else
		{
			// get the ConglomerateDescriptor for the index
			cd = dd.getConglomerateDescriptor(conglomerateName, sd, false);

			// Return an empty Properties if index does not exist
			if (cd == null) { return new Properties(); }

			conglomerateNumber = cd.getConglomerateNumber();
		}

		cc = tc.openConglomerate(
                conglomerateNumber,
                false,
                0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		properties = cc.getInternalTablePropertySet( new Properties() );

		cc.close();

		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}

        return properties;
	}
}
