/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.CatalogRowFactory

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

package org.apache.derby.iapi.sql.dictionary;

import java.util.Properties;

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.types.DataValueFactory;

/**
 * Superclass of all row factories.
 *
 *
 * @version 0.2
 */

public abstract	class CatalogRowFactory
{
	///////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////////


	protected 	String[] 			indexNames;
	protected 	int[][] 			indexColumnPositions;
	protected	boolean[]			indexUniqueness;

	protected	UUID				tableUUID;
	protected	UUID				heapUUID;
	protected	UUID[]				indexUUID;

	protected	DataValueFactory    dvf;
	private     final ExecutionFactory    ef;
	private		UUIDFactory			uuidf;

    private     int indexCount;
	private     int columnCount;
	private     String catalogName;

	///////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////////

    public CatalogRowFactory(UUIDFactory uuidf,
							 ExecutionFactory ef,
//IC see: https://issues.apache.org/jira/browse/DERBY-3147
							 DataValueFactory dvf)
								 
	{
		this.uuidf = uuidf;
		this.dvf = dvf;
		this.ef = ef;
	}

	/**
	  *	Gets a ExecutionFactory
	  *
	  *	@return	an execution factory
	  */
	public ExecutionFactory getExecutionFactory() {return ef;}

	/**
	  *	Get the UUID factory
	  *
	  *	@return	the UUID factory
	  */
	public	UUIDFactory	getUUIDFactory() { return uuidf; }

	/* Override the following methods in sub-classes if they have any
	 * indexes.
	 */

	 /**
	   *	Get the UUID of this catalog. This is the hard-coded uuid for
	   *	this catalog that is generated for releases starting with Plato (1.3).
	   *	Earlier releases generated their own UUIDs for system objectss on
	   *	the fly.
	   *
	   * @return	the name of this catalog
	   */
    public	UUID	getCanonicalTableUUID() { return tableUUID; }

	 /**
	   *	Get the UUID of the heap underlying this catalog. See getCanonicalTableUUID()
	   *	for a description of canonical uuids.
	   *
	   * @return	the uuid of the heap
	   */
	public	UUID	getCanonicalHeapUUID()  { return heapUUID; }

	 /**
	   *	Get the UUID of the numbered index. See getCanonicalTableUUID()
	   *	for a description of canonical uuids.
	   *
	   * @param	indexNumber	The (0-based) index number.
	   *
	   * @return	the uuid of the heap
	   */
	public	UUID	getCanonicalIndexUUID( int indexNumber )
	{
		return indexUUID[indexNumber];
	}

	/**
	 * Get the number of columns in the index for the specified index number.
	 *
	 * @param indexNum	The (0-based) index number.
	 *
	 * @return int		The number of columns in the index for the specifed index number.
	 */
	public int getIndexColumnCount(int indexNum)
	{
		return indexColumnPositions[indexNum].length;
	}

	/**
	 * Get the name for the heap conglomerate underlying this catalog.
	 * See getCanonicalTableUUID() for a description of canonical uuids.
	 *
	 * @return String	The name for the heap conglomerate.
	 */
	public String getCanonicalHeapName() { return catalogName + "_HEAP"; }

	/**
	 * Get the name for the specified index number.
	 *
	 * @param indexNum	The (0-based) index number.
	 *
	 * @return String	The name for the specified index number.
	 */
	public String getIndexName(int indexNum)
	{
		return indexNames[indexNum];
	}

	/**
	 * Return whether or not the specified index is unique.
	 *
	 * @param indexNumber	The (0-based) index number.
	 *
	 * @return boolean		Whether or not the specified index is unique.
	 */
	public boolean isIndexUnique(int indexNumber)
	{
		return (indexUniqueness != null ? indexUniqueness[indexNumber] : true);
	}

	/**
	  *	Gets the DataValueFactory for this connection.
	  *
	  *	@return	the data value factory for this connection
	  */
	public DataValueFactory	getDataValueFactory() { return dvf; }

	/**
	  *	Generate an index name based on the index number.
	  *
	  *	@param	indexNumber		Number of index
	  *
	  *	@return	the following index name: CatalogName + "_INDEX" + (indexNumber+1)
	  */
	public	String	generateIndexName( int indexNumber )
	{
		indexNumber++;
//IC see: https://issues.apache.org/jira/browse/DERBY-3147
		return	catalogName + "_INDEX" + indexNumber;
	}

	/** get the number of indexes on this catalog */
	public int getNumIndexes() { return indexCount; }

	/** get the name of the catalog */
	public String getCatalogName() { return catalogName; };

	/**
	  *	Initialize info, including array of index names and array of
	  * index column counts. Called at constructor time.
	  *
	  * @param  columnCount number of columns in the base table.
	  * @param  catalogName name of the catalog (the case might have to be converted).
	  * @param  indexColumnPositions 2 dim array of ints specifying the base
	  * column positions for each index.
	  *	@param	indexUniqueness		Uniqueness of the indices
	  *	@param	uuidStrings			Array of stringified UUIDs for table and its conglomerates
	  *
	  */
	public	void	initInfo(int        columnCount,
							 String 	catalogName,
							 int[][] 	indexColumnPositions,
							 boolean[] 	indexUniqueness,
							 String[]	uuidStrings)
							 
	{
		indexCount = (indexColumnPositions != null) ? 
			                 indexColumnPositions.length : 0;

//IC see: https://issues.apache.org/jira/browse/DERBY-3147
		this.catalogName = catalogName;
		this.columnCount = columnCount;

		UUIDFactory	uf = getUUIDFactory();
		this.tableUUID = uf.recreateUUID(uuidStrings[0] );
		this.heapUUID = uf.recreateUUID( uuidStrings[1] );

		if (indexCount > 0)
		{
			indexNames = new String[indexCount];
			indexUUID = new UUID[indexCount];
			for (int ictr = 0; ictr < indexCount; ictr++)
			{
				indexNames[ictr] = generateIndexName(ictr);
				indexUUID[ictr] = uf.recreateUUID(uuidStrings[ictr + 2 ]);
			}
//IC see: https://issues.apache.org/jira/browse/DERBY-6202
			this.indexColumnPositions = ArrayUtil.copy2( indexColumnPositions );
			this.indexUniqueness = ArrayUtil.copy( indexUniqueness );
 
		}
	}

	/**
	 * Get the Properties associated with creating the heap.
	 *
	 * @return The Properties associated with creating the heap.
	 */
	public Properties getCreateHeapProperties()
	{
		Properties properties = new Properties();
		// default properties for system tables:
		properties.put(Property.PAGE_SIZE_PARAMETER,"1024");
		properties.put(RawStoreFactory.PAGE_RESERVED_SPACE_PARAMETER,"0");
		properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,"1");
		return properties;
	}

	/**
	 * Get the Properties associated with creating the specified index.
	 *
	 * @param indexNumber	The specified index number.
	 *
	 * @return The Properties associated with creating the specified index.
	 */
	public Properties getCreateIndexProperties(int indexNumber)
	{
		Properties properties = new Properties();
		// default properties for system indexes:
		properties.put(Property.PAGE_SIZE_PARAMETER,"1024");
		return properties;
	}

	/**
	  *	Get the index number for the primary key index on this catalog.
	  *
	  *	@return	a 0-based number
	  *
	  */
	public	int	getPrimaryKeyIndexNumber()
	{
		if (SanityManager.DEBUG)
			SanityManager.NOTREACHED();
		return 0;
	}

	/**
	 * Get the number of columns in the heap.
	 *
	 * @return The number of columns in the heap.
	 */
    public int getHeapColumnCount() throws StandardException
	{
		return columnCount;
	}

	/** 
	 * Return an empty row for this conglomerate. 
	 */
	public  ExecRow makeEmptyRow() throws StandardException
	{
 		return	this.makeRow(null, null);
	}

    /**
     * <p>
     * Create an empty row for this conglomerate, in the format that would
     * be used in a database that was created with, or hard upgraded to,
     * the currently running version. That is, even if the database is only
     * soft-upgraded, this method should return a row in the new format.
     * </p>
     *
     * <p>
     * This method is for use in code that creates the catalogs, or that
     * upgrades the format of the catalogs to the newest version. Other code
     * should call {@link #makeEmptyRow()}, which returns a row in the format
     * used in the old database version if the database is soft-upgraded.
     * </p>
     *
     * @return an empty row
     * @throws StandardException if an error happens when creating the row
     */
    public ExecRow makeEmptyRowForCurrentVersion() throws StandardException {
        return makeEmptyRow();
    }

	/**
	 * most subclasses should provide this method. One or two oddball cases in
	 * Replication and SysSTATEMENTSRowFactory don't. For those we call makeRow
	 * with the additional arguments.
	 */
	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException
	{
		if (SanityManager.DEBUG) { SanityManager.THROWASSERT( "Should not get here." ); }
		return null;
	}

	// abstract classes that should be implemented by subclasses. 

	/** builds a tuple descriptor from a row */
	public abstract TupleDescriptor 
		buildDescriptor(ExecRow row,
						TupleDescriptor parentTuple,
						DataDictionary	dataDictionary)
		throws StandardException;

	/** builds a column list for the catalog */
	public abstract SystemColumn[]	buildColumnList() throws StandardException;

	/** Return the column positions for a given index number */
	public int[] getIndexColumnPositions(int indexNumber)
	{
		return indexColumnPositions[indexNumber];
	}	
}
