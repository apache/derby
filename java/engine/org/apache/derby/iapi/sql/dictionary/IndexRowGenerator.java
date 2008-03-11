/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.IndexRowGenerator

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.catalog.types.IndexDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;

/**
 * This class extends IndexDescriptor for internal use by the
 * DataDictionary.
 */
public class IndexRowGenerator implements IndexDescriptor, Formatable
{
	private IndexDescriptor	id;
	private ExecutionFactory ef;

	/**
	 * Constructor for an IndexRowGeneratorImpl
	 *
	 * @param indexType		The type of index
	 * @param isUnique		True means the index is unique
	 * @param baseColumnPositions	An array of column positions in the base
	 *								table.  Each index column corresponds to a
	 *								column position in the base table.
	 * @param isAscending	An array of booleans telling asc/desc on each
	 *						column.
	 * @param numberOfOrderedColumns	In the future, it will be possible
	 *									to store non-ordered columns in an
	 *									index.  These will be useful for
	 *									covered queries.
	 */
	public IndexRowGenerator(String indexType,
								boolean isUnique,
								int[] baseColumnPositions,
								boolean[] isAscending,
								int numberOfOrderedColumns)
	{
		id = new IndexDescriptorImpl(
                        indexType,
                        isUnique, //default uniqueWithDuplicateNulls to false
                        false,
                        baseColumnPositions,
                        isAscending,
                        numberOfOrderedColumns);

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(baseColumnPositions != null,
				"baseColumnPositions are null");
		}
	}
        
    /**
     * Constructor for an IndexRowGeneratorImpl
     * 
     * @param indexType		The type of index
     * @param isUnique		True means the index is unique
     * @param isUniqueWithDuplicateNulls means the index is almost unique
     *                              i.e. unique only for non null keys
     * @param baseColumnPositions	An array of column positions in the base
     * 								table.  Each index column corresponds to a
     * 								column position in the base table.
     * @param isAscending	An array of booleans telling asc/desc on each
     * 						column.
     * @param numberOfOrderedColumns	In the future, it will be possible
     * 									to store non-ordered columns in an
     * 									index.  These will be useful for
     * 									covered queries.
     */
	public IndexRowGenerator(String indexType,
								boolean isUnique,
								boolean isUniqueWithDuplicateNulls,
								int[] baseColumnPositions,
								boolean[] isAscending,
								int numberOfOrderedColumns)
	{
		id = new IndexDescriptorImpl(indexType,
									isUnique,
									isUniqueWithDuplicateNulls,
									baseColumnPositions,
									isAscending,
									numberOfOrderedColumns);

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(baseColumnPositions != null,
				"baseColumnPositions are null");
		}
	}

	/**
	 * Constructor for an IndexRowGeneratorImpl
	 *
	 * @param indexDescriptor		An IndexDescriptor to delegate calls to
	 */
	public IndexRowGenerator(IndexDescriptor indexDescriptor)
	{
		id = indexDescriptor;
	}

	/**
	 * Get a template for the index row, to be used with getIndexRow.
	 *
	 * @return	A row template for the index row.
	 */
	public ExecIndexRow getIndexRowTemplate()
	{
		return getExecutionFactory().getIndexableRow(
										id.baseColumnPositions().length + 1);
	}

	/**
	 * Get an index row for this index given a row from the base table
	 * and the RowLocation of the base row.  This method can be used
	 * to get the new index row for inserts, and the old and new index
	 * rows for deletes and updates.  For updates, the result row has
	 * all the old column values followed by all of the new column values,
	 * so you must form a row using the new column values to pass to
	 * this method to get the new index row.
	 *
	 * @param baseRow	A row in the base table
	 * @param rowLocation	The RowLocation of the row in the base table
	 * @param indexRow	A template for the index row.  It must have the
	 *					correct number of columns.
	 * @param bitSet	If non-null, then baseRow is a partial row and the
	 *					set bits in bitSet represents the column mapping for
	 *					the partial row to the complete base row. <B> WARNING:
	 *					</B> ONE based!!!
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void getIndexRow(ExecRow baseRow,
							RowLocation rowLocation,
							ExecIndexRow indexRow,
							FormatableBitSet bitSet)
						throws StandardException
	{
		/*
		** Set the columns in the index row that are based on columns in
		** the base row.
		*/
		int[] baseColumnPositions = id.baseColumnPositions();
		int colCount = baseColumnPositions.length;

		if (bitSet == null)
		{
			/*
			** Set the columns in the index row that are based on columns in
			** the base row.
			*/
			for (int i = 0; i < colCount ; i++)
			{
				indexRow.setColumn(i + 1,
						baseRow.getColumn(baseColumnPositions[i]));
			}
		}
		else
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(!bitSet.get(0), "element zero of the bitSet passed into getIndexRow() is not false, bitSet should be 1 based");
			}
 
			/*
			** Set the columns in the index row that are based on columns in
			** the base row.
			*/
			for (int i = 0; i < colCount; i++)
			{
				int fullColumnNumber = baseColumnPositions[i];
				int partialColumnNumber = 0;
				for (int index = 1; index <= fullColumnNumber; index++)
				{
					if (bitSet.get(index))
					{
						partialColumnNumber++;
					}
				}
				indexRow.setColumn(i + 1,
							baseRow.getColumn(partialColumnNumber));
			}
		}

		/* Set the row location in the last column of the index row */
		indexRow.setColumn(colCount + 1, rowLocation);
	}

    /**
     * Return an array of collation ids for this table.
     * <p>
     * Return an array of collation ids, one for each column in the
     * columnDescriptorList.  This is useful for passing collation id info
     * down to store, for instance in createConglomerate() to create
     * the index.
     *
     * This is only expected to get called during ddl, so object allocation
     * is ok. 
     *
	 * @param columnList ColumnDescriptors describing the base table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int[] getColumnCollationIds(ColumnDescriptorList columnList)
		throws StandardException
    {
        int[] base_cols     = id.baseColumnPositions();
        int[] collation_ids = new int[base_cols.length + 1];

		for (int i = 0; i < base_cols.length; i++)
		{
            collation_ids[i] =
				columnList.elementAt(
                    base_cols[i] - 1).getType().getCollationType();
		}

        // row location column at end is always basic collation type.
        collation_ids[collation_ids.length - 1] = 
            StringDataValue.COLLATION_TYPE_UCS_BASIC; 

		return(collation_ids);
    }

		 
	/**
	 * Get the IndexDescriptor that this IndexRowGenerator is based on.
	 */
	public IndexDescriptor getIndexDescriptor()
	{
		return id;
	}

	/** Zero-argument constructor for Formatable interface */
	public IndexRowGenerator()
	{
	}

	/**
     * @see IndexDescriptor#isUniqueWithDuplicateNulls
     */
	public boolean isUniqueWithDuplicateNulls()
	{
		return id.isUniqueWithDuplicateNulls();
	}
	/** @see IndexDescriptor#isUnique */
	public boolean isUnique()
	{
		return id.isUnique();
	}

	/** @see IndexDescriptor#baseColumnPositions */
	public int[] baseColumnPositions()
	{
		return id.baseColumnPositions();
	}

	/** @see IndexDescriptor#getKeyColumnPosition */
	public Integer getKeyColumnPosition(Integer heapColumnPosition)
	{
		return id.getKeyColumnPosition(heapColumnPosition);
	}

	/** @see IndexDescriptor#getKeyColumnPosition */
	public int getKeyColumnPosition(int heapColumnPosition)
	{
		return id.getKeyColumnPosition(heapColumnPosition);
	}

	/** @see IndexDescriptor#numberOfOrderedColumns */
	public int numberOfOrderedColumns()
	{
		return id.numberOfOrderedColumns();
	}

	/** @see IndexDescriptor#indexType */
	public String indexType()
	{
		return id.indexType();
	}

	public String toString()
	{
		return id.toString();
	}

	/** @see IndexDescriptor#isAscending */
	public boolean			isAscending(Integer keyColumnPosition)
	{
		return id.isAscending(keyColumnPosition);
	}

	/** @see IndexDescriptor#isDescending */
	public boolean			isDescending(Integer keyColumnPosition)
	{
		return id.isDescending(keyColumnPosition);
	}

	/** @see IndexDescriptor#isAscending */
	public boolean[]		isAscending()
	{
		return id.isAscending();
	}

	/** @see IndexDescriptor#setBaseColumnPositions */
	public void		setBaseColumnPositions(int[] baseColumnPositions)
	{
		id.setBaseColumnPositions(baseColumnPositions);
	}

	/** @see IndexDescriptor#setIsAscending */
	public void		setIsAscending(boolean[] isAscending)
	{
		id.setIsAscending(isAscending);
	}

	/** @see IndexDescriptor#setNumberOfOrderedColumns */
	public void		setNumberOfOrderedColumns(int numberOfOrderedColumns)
	{
		id.setNumberOfOrderedColumns(numberOfOrderedColumns);
	}

	/**
	 * Test for value equality
	 *
	 * @param other		The other indexrowgenerator to compare this one with
	 *
	 * @return	true if this indexrowgenerator has the same value as other
	 */

	public boolean equals(Object other)
	{
		return id.equals(other);
	}

	/**
	  @see java.lang.Object#hashCode
	  */
	public int hashCode()
	{
		return id.hashCode();
	}

	private ExecutionFactory getExecutionFactory()
	{
		if (ef == null)
		{
			ExecutionContext	ec;

			ec = (ExecutionContext)
					ContextService.getContext(ExecutionContext.CONTEXT_ID);
			ef = ec.getExecutionFactory();
		}
		return ef;
	}

	////////////////////////////////////////////////////////////////////////////
	//
	// EXTERNALIZABLE
	//
	////////////////////////////////////////////////////////////////////////////

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on read error
	 * @exception ClassNotFoundException	Thrown on read error
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		id = (IndexDescriptor)in.readObject();
	}

	/**
	 *
	 * @exception IOException	Thrown on write error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(id);
	}

	/* TypedFormat interface */
	public int getTypeFormatId()
	{
		return StoredFormatIds.INDEX_ROW_GENERATOR_V01_ID;
	}

}
