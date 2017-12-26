/*

   Derby - Class org.apache.derby.catalog.types.IndexDescriptorImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog.types;

import org.apache.derby.catalog.IndexDescriptor;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/**
 * See also {@link org.apache.derby.iapi.sql.dictionary.IndexRowGenerator}.
 * <p>
 * For a description of how deferrable and non-deferrable constraints
 * are backed differently, including the meaning of the
 * boolean attributes used here, see {@link
 * org.apache.derby.catalog.IndexDescriptor}.
 */
public class IndexDescriptorImpl implements IndexDescriptor, Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
    **  If, in between releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private boolean		isUnique;
	private int[]		baseColumnPositions;
	private boolean[]	isAscending;
	private int			numberOfOrderedColumns;
	private String		indexType;
	//attribute to indicate the indicates allows duplicate only in
	//case of non null keys. This attribute has no effect if the isUnique
    //is true. If isUnique is false and isUniqueWithDuplicateNulls is set 
    //to true the index will allow duplicate nulls but for non null keys 
    //will act like a unique index.
	private boolean     isUniqueWithDuplicateNulls;

    /**
     * The index represents a PRIMARY KEY or a UNIQUE NOT NULL constraint which
     * is deferrable.
     * {@code true} implies {@code isUnique == false} and
     * {@code isUniqueWithDuplicateNulls == false} and
     * {@code hasDeferrableChecking == true}.
     */
    private boolean     isUniqueDeferrable;

    /**
     * The index represents a constraint which is deferrable.
     */
    private boolean     hasDeferrableChecking;

	/**
     * Constructor for an IndexDescriptorImpl
     * 
     * @param indexType		The type of index
     * @param isUnique		True means the index is unique
     * @param isUniqueWithDuplicateNulls True means the index will be unique
     *                              for non null values but duplicate nulls
     *                              will be allowed.
     *                              This parameter has no effect if the isUnique
     *                              is true. If isUnique is false and 
     *                              isUniqueWithDuplicateNulls is set to true the
     *                              index will allow duplicate nulls but for
     *                              non null keys will act like a unique index.
     * @param isUniqueDeferrable    True means the index represents a PRIMARY
     *                              KEY or a UNIQUE NOT NULL constraint which
     *                              is deferrable.
     * @param hasDeferrableChecking True if this index supports a deferrable
     *                              constraint.
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
	public IndexDescriptorImpl(String indexType,
								boolean isUnique,
								boolean isUniqueWithDuplicateNulls,
                                boolean isUniqueDeferrable,
                                boolean hasDeferrableChecking,
								int[] baseColumnPositions,
								boolean[] isAscending,
								int numberOfOrderedColumns)
	{
		this.indexType = indexType;
		this.isUnique = isUnique;
		this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.isUniqueDeferrable = isUniqueDeferrable;
        this.hasDeferrableChecking = hasDeferrableChecking;
		this.baseColumnPositions = ArrayUtil.copy( baseColumnPositions );
		this.isAscending = ArrayUtil.copy( isAscending );
		this.numberOfOrderedColumns = numberOfOrderedColumns;
	}

	/** Zero-argument constructor for Formatable interface */
	public IndexDescriptorImpl()
	{
	}

	/**
     * 
     * 
     * @see IndexDescriptor#isUniqueWithDuplicateNulls
     */
	public boolean isUniqueWithDuplicateNulls()
	{
		return isUniqueWithDuplicateNulls;
	}

    /**
     *
     * @return  {@code true} is the index supports a deferrable constraint
     */
    public boolean hasDeferrableChecking()
    {
        return hasDeferrableChecking;
    }

    /**
     * The index represents a PRIMARY KEY or a UNIQUE NOT NULL constraint which
     * is deferrable.
     * {@code true} implies {@code #isUnique() == false} and
     * {@code #isUniqueWithDuplicateNulls() == false} and
     * {@code #hasDeferrableChecking() == true}.
     *
     * @return {@code true} is the index supports such a constraint
     */
    public boolean isUniqueDeferrable()
    {
        return isUniqueDeferrable;
    }

	/** @see IndexDescriptor#isUnique */
	public boolean isUnique()
	{
		return isUnique;
	}

	/** @see IndexDescriptor#baseColumnPositions */
	public int[] baseColumnPositions()
	{
        return ArrayUtil.copy( baseColumnPositions );
	}

	/** @see IndexDescriptor#getKeyColumnPosition */
	public int getKeyColumnPosition(int heapColumnPosition)
	{
		/* Return 0 if column is not in the key */
		int keyPosition = 0;

		for (int index = 0; index < baseColumnPositions.length; index++)
		{
			/* Return 1-based key column position if column is in the key */
			if (baseColumnPositions[index] == heapColumnPosition)
			{
				keyPosition = index + 1;
				break;
			}
		}

		return keyPosition;
	}

	/** @see IndexDescriptor#numberOfOrderedColumns */
	public int numberOfOrderedColumns()
	{
		return numberOfOrderedColumns;
	}

	/** @see IndexDescriptor#indexType */
	public String indexType()
	{
		return indexType;
	}

	/** @see IndexDescriptor#isAscending */
	public boolean			isAscending(Integer keyColumnPosition)
	{
		int i = keyColumnPosition.intValue() - 1;
		if (i < 0 || i >= baseColumnPositions.length)
			return false;
		return isAscending[i];
	}

	/** @see IndexDescriptor#isDescending */
	public boolean			isDescending(Integer keyColumnPosition)
	{
		int i = keyColumnPosition.intValue() - 1;
		if (i < 0 || i >= baseColumnPositions.length)
			return false;
		return ! isAscending[i];
	}

	/** @see IndexDescriptor#isAscending */
	public boolean[]		isAscending()
	{
        return ArrayUtil.copy( isAscending );
	}

	/** @see IndexDescriptor#setBaseColumnPositions */
	public void		setBaseColumnPositions(int[] baseColumnPositions)
	{
		this.baseColumnPositions = ArrayUtil.copy( baseColumnPositions );
	}

	/** @see IndexDescriptor#setIsAscending */
	public void		setIsAscending(boolean[] isAscending)
	{
		this.isAscending = ArrayUtil.copy( isAscending );
	}

	/** @see IndexDescriptor#setNumberOfOrderedColumns */
	public void		setNumberOfOrderedColumns(int numberOfOrderedColumns)
	{
		this.numberOfOrderedColumns = numberOfOrderedColumns;
	}

	public String toString()
	{
        StringBuilder   sb = new StringBuilder(60);

        if (isUnique || isUniqueDeferrable)
			sb.append("UNIQUE ");
		else if (isUniqueWithDuplicateNulls)
            sb.append ("UNIQUE WITH DUPLICATE NULLS ");

        if (hasDeferrableChecking) {
            sb.append(" DEFERRABLE CHECKING ");
        }

		sb.append(indexType);

		sb.append(" (");


		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			if (i > 0)
				sb.append(", ");
			sb.append(baseColumnPositions[i]);
			if (! isAscending[i])
				sb.append(" DESC");
		}

		sb.append(")");

		return sb.toString();
	}

	/* Externalizable interface */

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on read error
	 */
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
	{
		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		isUnique = fh.getBoolean("isUnique");
		int bcpLength = fh.getInt("keyLength");
		baseColumnPositions = new int[bcpLength];
		isAscending = new boolean[bcpLength];
		for (int i = 0; i < bcpLength; i++)
		{
			baseColumnPositions[i] = fh.getInt("bcp" + i);
			isAscending[i] = fh.getBoolean("isAsc" + i);
		}
		numberOfOrderedColumns = fh.getInt("orderedColumns");
		indexType = (String)fh.get("indexType");
		//isUniqueWithDuplicateNulls attribute won't be present if the index
		//was created in older versions  
		if (fh.containsKey("isUniqueWithDuplicateNulls"))
			isUniqueWithDuplicateNulls = fh.getBoolean(
                                    "isUniqueWithDuplicateNulls");
		else
			isUniqueWithDuplicateNulls = false;

        // hasDeferrableChecking won't be present if the index
        // was created in old versions (< 10_11).
        if (fh.containsKey("hasDeferrableChecking")) {
            hasDeferrableChecking = fh.getBoolean("hasDeferrableChecking");
        } else {
            hasDeferrableChecking = false;
        }

        // isUniqueDeferrable won't be present if the index
        // was created in old versions (< 10_11).
        if (fh.containsKey("isUniqueDeferrable")) {
            isUniqueDeferrable = fh.getBoolean("isUniqueDeferrable");
        } else {
            isUniqueDeferrable = false;
        }
	}

	/**
	 * @see java.io.Externalizable#writeExternal
	 *
	 * @exception IOException	Thrown on write error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		FormatableHashtable fh = new FormatableHashtable();
		fh.putBoolean("isUnique", isUnique);
		fh.putInt("keyLength", baseColumnPositions.length);
		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			fh.putInt("bcp" + i, baseColumnPositions[i]);
			fh.putBoolean("isAsc" + i, isAscending[i]);
		}
		fh.putInt("orderedColumns", numberOfOrderedColumns);
		fh.put("indexType", indexType);
		//write the new attribut older versions will simply ignore it
		fh.putBoolean("isUniqueWithDuplicateNulls", 
                                        isUniqueWithDuplicateNulls);
        fh.putBoolean("hasDeferrableChecking", hasDeferrableChecking);
        fh.putBoolean("isUniqueDeferrable", isUniqueDeferrable);
        out.writeObject(fh);
	}

	/* TypedFormat interface */
	public int getTypeFormatId()
	{
		return StoredFormatIds.INDEX_DESCRIPTOR_IMPL_V02_ID;
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
		/* Assume not equal until we know otherwise */
		boolean retval = false;

		/* Equal only if comparing the same class */
		if (other instanceof IndexDescriptorImpl)
		{
			IndexDescriptorImpl id = (IndexDescriptorImpl) other;

			/*
			** Check all the fields for equality except for the array
			** elements (this is hardest, so save for last)
			*/
			if ((id.isUnique == this.isUnique)       &&
                (id.isUniqueWithDuplicateNulls == 
                    this.isUniqueWithDuplicateNulls) &&
                (id.baseColumnPositions.length ==
                    this.baseColumnPositions.length) &&
                (id.numberOfOrderedColumns     == 
                    this.numberOfOrderedColumns)     &&
                (id.indexType.equals(this.indexType)))
			{
				/*
				** Everything but array elements known to be true -
				** Assume equal, and check whether array elements are equal.
				*/
				retval = true;

				for (int i = 0; i < this.baseColumnPositions.length; i++)
				{
					/* If any array element is not equal, return false */
					if ((id.baseColumnPositions[i] !=
						 this.baseColumnPositions[i]) || 
                        (id.isAscending[i] != this.isAscending[i]))
					{
						retval = false;
						break;
					}
				}
			}
		}

		return retval;
	}

	/**
	  @see java.lang.Object#hashCode
	  */
	public int hashCode()
	{
		int	retval;

		retval = isUnique ? 1 : 2;
		retval *= numberOfOrderedColumns;
		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			retval *= baseColumnPositions[i];
		}
		retval *= indexType.hashCode();

		return retval;
	}
}
