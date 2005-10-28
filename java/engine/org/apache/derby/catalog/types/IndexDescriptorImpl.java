/*

   Derby - Class org.apache.derby.catalog.types.IndexDescriptorImpl

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

package org.apache.derby.catalog.types;

import org.apache.derby.catalog.IndexDescriptor;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;

/** @see IndexRowGenerator */
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
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private boolean		isUnique;
	private int[]		baseColumnPositions;
	private boolean[]	isAscending;
	private int			numberOfOrderedColumns;
	private String		indexType;

	/**
	 * Constructor for an IndexDescriptorImpl
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
	public IndexDescriptorImpl(String indexType,
								boolean isUnique,
								int[] baseColumnPositions,
								boolean[] isAscending,
								int numberOfOrderedColumns)
	{
		this.indexType = indexType;
		this.isUnique = isUnique;
		this.baseColumnPositions = baseColumnPositions;
		this.isAscending = isAscending;
		this.numberOfOrderedColumns = numberOfOrderedColumns;
	}

	/** Zero-argument constructor for Formatable interface */
	public IndexDescriptorImpl()
	{
	}

	/** @see IndexDescriptor#isUnique */
	public boolean isUnique()
	{
		return isUnique;
	}

	/** @see IndexDescriptor#baseColumnPositions */
	public int[] baseColumnPositions()
	{
		return baseColumnPositions;
	}

	/** @see IndexDescriptor#getKeyColumnPosition */
	public Integer getKeyColumnPosition(Integer heapColumnPosition)
	{
		return new Integer(getKeyColumnPosition(heapColumnPosition.intValue()));
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
		return isAscending;
	}

	/** @see IndexDescriptor#setBaseColumnPositions */
	public void		setBaseColumnPositions(int[] baseColumnPositions)
	{
		this.baseColumnPositions = baseColumnPositions;
	}

	/** @see IndexDescriptor#setIsAscending */
	public void		setIsAscending(boolean[] isAscending)
	{
		this.isAscending = isAscending;
	}

	/** @see IndexDescriptor#setNumberOfOrderedColumns */
	public void		setNumberOfOrderedColumns(int numberOfOrderedColumns)
	{
		this.numberOfOrderedColumns = numberOfOrderedColumns;
	}

	public String toString()
	{
		String	uniqueness;
		String	cols;

		StringBuffer	sb = new StringBuffer(60);

		if (isUnique)
			sb.append("UNIQUE ");

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
			if ((id.isUnique == this.isUnique) &&
				(id.baseColumnPositions.length ==
										this.baseColumnPositions.length) &&
				(id.numberOfOrderedColumns == this.numberOfOrderedColumns) &&
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
						this.baseColumnPositions[i]) || (id.isAscending[i] !=
						this.isAscending[i]))
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
