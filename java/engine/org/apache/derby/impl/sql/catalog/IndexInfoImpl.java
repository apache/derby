/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.catalog.UUID;

/**
* A poor mans structure used in DataDictionaryImpl.java.
* Used to save information about system indexes.
*
* @author jerry
*/
public class IndexInfoImpl
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	boolean				isUnique;
	String[]			columnNames;
	int[]				columnPositions;
	IndexRowGenerator	irg;
	int					columnCount;
	long				conglomerateNumber;
	String				name;

	/**
	 * Constructor
	 *
	 * @param conglomerateNumber	The conglomerate number for the index
	 * @param indexName				The name of the index
	 * @param columnCount			The number of columns in the index
	 * @param isUnique				Whether or not the index was declared as unique
	 * @param indexNumber			(0-based) number of index within catalog's indexes
	 * @param crf					CatalogRowFactory for the catalog
	 */
	IndexInfoImpl(long conglomerateNumber, String indexName, int columnCount,
				  boolean isUnique,
				  int indexNumber, CatalogRowFactory crf)
	{
		this.conglomerateNumber = conglomerateNumber;
		name = indexName;
		this.columnCount = columnCount;
		this.isUnique = isUnique;
		columnNames = crf.getIndexColumnNames(indexNumber);
		columnPositions = crf.getIndexColumnPositions(indexNumber);
	}

	/**
	 * Get the conglomerate number for the index.
	 *
	 * @return long	The conglomerate number for the index.
	 */
	public long getConglomerateNumber()
	{
		return conglomerateNumber;
	}

	/**
	 * Set the conglomerate number for the index.
	 *
	 * @param conglomerateNumber	The conglomerateNumber for the index.
	 *
	 * @return	Nothing.
	 */
	public void setConglomerateNumber(long conglomerateNumber)
	{
		this.conglomerateNumber = conglomerateNumber;
	}

	/**
	 * Get the index name for the index.
	 *
	 * @return String	The index name for the index.
	 */
	public String getIndexName()
	{
		return name;
	}

	/**
	 * Set the name for the index.
	 *
	 * @param indexName		The name for the index.
	 *
	 * @return	Nothing.
	 */
	public void setIndexName(String indexName)
	{
		name = indexName;
	}

	/**
	 * Get the column count for the index.
	 *
	 * @return int	The column count for the index.
	 */
	public int getColumnCount()
	{
		return columnCount;
	}

	/**
	 * Get the IndexRowGenerator for this index.
	 *
	 * @return IndexRowGenerator	The IRG for this index.
	 */
	public IndexRowGenerator getIndexRowGenerator()
	{
		return irg;
	}

	/**
	 * Set the IndexRowGenerator for this index.
	 *
	 * @param irg			The IndexRowGenerator for this index.
	 *
	 * @return Nothing.
	 */
	public void setIndexRowGenerator(IndexRowGenerator irg)
	{
		this.irg = irg;
	}

	/**
	 * Get the base column position for a column within a catalog
	 * given the (0-based) column number for the column within the index.
	 *
	 * @param colNumber		The column number within the index
	 *
	 * @return int		The base column position for the column.
	 */
	public int getBaseColumnPosition(int colNumber)
	{
		return columnPositions[colNumber];
	}

	/**
	 * Set the base column position for a column within a catalog
	 * given the (0-based) column number for the column within the index.
	 *
	 * @param colNumber		The column number within the index
	 * @param baseColPos	The base column position for the column.
	 *
	 * @return Nothing.
	 */
	public void setBaseColumnPosition(int colNumber,
									 int baseColumnPosition)
	{
		columnPositions[colNumber] = baseColumnPosition;
	}

	/**
	 * Return whether or not this index is declared unique
	 *
	 * @return boolean		Whether or not this index is declared unique
	 */
	public boolean isIndexUnique()
	{
		return isUnique;
	}
}
