/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

/**
 *	This class is the superclass for the classes that describe actions 
 *  that are ALWAYS performed for a CREATE/DROP INDEX Statement at Execution time.
 *
 *	@author Jerry Brenner
 */

public abstract class IndexConstantAction extends DDLSingleTableConstantAction
{

	String				indexName;
	String				tableName;
	String				schemaName;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	sd					Schema that index lives in.
	 *
	 */
	protected	IndexConstantAction(
								UUID				tableId,
								String				indexName,
								String				tableName,
								String schemaName)
	{
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
		}
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { return indexName; }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 *
	 * @return Nothing.
	 */
	public void setIndexName(String indexName)
	{
		this.indexName = indexName;
	}
}
