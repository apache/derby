/*

   Derby - Class org.apache.derby.iapi.db.PropertyInfo

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

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.SQLState;

import java.util.Properties;
import java.sql.SQLException;

/**
  *	PropertyInfo is a class with static methods that retrieve the properties
  * associated with a table or index and set and retrieve properties associated
  * with a database.
  * 
  * <P>
  This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
  <p>This class can be accessed using the class alias <code> PROPERTYINFO </code> in SQL-J statements.
  * <P>
  * <I>IBM Corp. reserves the right to change, rename, or
  * remove this interface at any time.</I>
  */
public final class PropertyInfo
{

    /**
     * Get the Properties associated with a given table.
     *
	 * @param schemaName    The name of the schema that the table is in.
	 * @param tableName     The name of the table.
	 * 
	 * @return Properties   The Properties associated with the specified table.
     *                      (An empty Properties is returned if the table does not exist.)
     * @exception SQLException on error
     */
    public static Properties getTableProperties(String schemaName, String tableName)
        throws SQLException
	{
		return	PropertyInfo.getConglomerateProperties( schemaName, tableName, false );
	}

    /**
     * Get the Properties associated with a given index.
     *
	 * @param schemaName    The name of the schema that the index is in.
	 * @param indexName     The name of the index.
	 * 
	 * @return Properties   The Properties associated with the specified index.
     *                      (An empty Properties is returned if the index does not exist.)
     * @exception SQLException on error
     */
    public static Properties getIndexProperties(String schemaName, String indexName)
        throws SQLException
	{
		return	PropertyInfo.getConglomerateProperties( schemaName, indexName, true );
	}

	/**
		Fetch the value of a property of the database on the current connection.

		@param key the property key

		@return the value of the property or null if the property is not set.

		@exception SQLException on error
	*/
	public static String getDatabaseProperty(String key) throws SQLException {
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

		try {
			return PropertyUtil.getDatabaseProperty(lcc.getTransactionExecute(), key);
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
		Set or delete the value of a property of the database on the current connection.

		@param key the property key
		@param value the new value, if null the property is deleted.

		@exception SQLException on error
	*/
	public static void setDatabaseProperty(String key, String value) throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

		try {
		Authorizer a = lcc.getAuthorizer();
		a.authorize(Authorizer.PROPERTY_WRITE_OP);

        // Get the current transaction controller
        TransactionController tc = lcc.getTransactionExecute();

		tc.setProperty(key, value, false);
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
	  Internal use only.
	  */
    private	PropertyInfo() {}


	//////////////////////////////////////////////////////////////////////////////
	//
	//	PRIVATE METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

    /**
     * Get the Properties associated with a given conglomerate
     *
	 * @param schemaName    	The name of the schema that the conglomerate is in.
	 * @param conglomerateName  The name of the conglomerate.
	 * @param conglomerateType	TABLE or INDEX.
	 * 
	 * @return Properties   The Properties associated with the specified conglomerate.
     *                      (An empty Properties is returned if the conglomerate does not exist.)
     * @exception SQLException on error
     */
	private static Properties	getConglomerateProperties( String schemaName, String conglomerateName, boolean isIndex )
        throws SQLException
	{
		long					  conglomerateNumber;

        // find the language context.
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

        // Get the current transaction controller
        TransactionController tc = lcc.getTransactionExecute();

		try {

		// find the DataDictionary
		DataDictionary dd = lcc.getDataDictionary();


		// get the SchemaDescriptor
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
		if ( !isIndex)
		{
			// get the TableDescriptor for the table
			TableDescriptor td = dd.getTableDescriptor(conglomerateName, sd);

			// Return an empty Properties if table does not exist or if it is for a view.
			if ((td == null) || td.getTableType() == TableDescriptor.VIEW_TYPE) { return new Properties(); }

			conglomerateNumber = td.getHeapConglomerateId();
		}
		else
		{
			// get the ConglomerateDescriptor for the index
			ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateName, sd, false);

			// Return an empty Properties if index does not exist
			if (cd == null) { return new Properties(); }

			conglomerateNumber = cd.getConglomerateNumber();
		}

		ConglomerateController cc = tc.openConglomerate(
                conglomerateNumber,
                false,
                0, 
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_SERIALIZABLE);

		Properties properties = tc.getUserCreateConglomPropList();
		cc.getTableProperties( properties );

		cc.close();
        return properties;

		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}

	}
}
