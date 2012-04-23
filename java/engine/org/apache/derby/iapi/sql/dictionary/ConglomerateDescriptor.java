/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor

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

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Provider;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

/**
 * The ConglomerateDescriptor class is used to get information about
 * conglomerates for the purpose of optimization.
 * 
 * A ConglomerateDescriptor can map to a base table, an index
 * or a index backing a constraint. Multiple ConglomerateDescriptors
 * can map to a single underlying store conglomerate, such as when
 * multiple index definitions share a physical file.
 *
 * NOTE: The language module does not have to know much about conglomerates
 * with this architecture. To get the cost of using a conglomerate, all it
 * has to do is pass the ConglomerateDescriptor to the access methods, along
 * with the predicate. What the access methods need from a
 * ConglomerateDescriptor remains to be seen.
 * 
 * 
 *
 * @version 0.1
 */

public final class ConglomerateDescriptor extends TupleDescriptor
	implements UniqueTupleDescriptor, Provider
{
	// Implementation
	private long	conglomerateNumber;
	private String	name;
	private transient String[]	columnNames;
	private final boolean	indexable;
	private final boolean	forConstraint;
	private final IndexRowGenerator	indexRowGenerator;
	private final UUID	uuid;
	private final UUID	tableID;
	private final UUID	schemaID;

	/**
	 * Constructor for a conglomerate descriptor.
	 *
	 * @param dataDictionary		The data dictionary that this descriptor lives in
	 * @param conglomerateNumber	The number for the conglomerate
	 *				we're interested in
	 * @param name			The name of the conglomerate, if any
	 * @param indexable		TRUE means the conglomerate is indexable,
	 *				FALSE means it isn't
	 * @param indexRowGenerator	The descriptor of the index if it's not a
	 *							heap
	 * @param forConstraint		TRUE means the conglomerate is an index backing up
	 *							a constraint, FALSE means it isn't
	 * @param uuid		UUID  for this conglomerate
	 * @param tableID	UUID for the table that this conglomerate belongs to
	 * @param schemaID	UUID for the schema that this conglomerate belongs to
	 */
	ConglomerateDescriptor(DataDictionary dataDictionary,
							   long conglomerateNumber,
							   String name,
							   boolean indexable,
							   IndexRowGenerator indexRowGenerator,
							   boolean forConstraint,
							   UUID uuid,
							   UUID tableID,
							   UUID schemaID)
	{
		super( dataDictionary );

		this.conglomerateNumber = conglomerateNumber;
		this.name = name;
		this.indexable = indexable;
		this.indexRowGenerator = indexRowGenerator;
		this.forConstraint = forConstraint;
		if (uuid == null)
		{
			UUIDFactory uuidFactory = Monitor.getMonitor().getUUIDFactory();
			uuid = uuidFactory.createUUID();
		}
		this.uuid = uuid;
		this.tableID = tableID;
		this.schemaID = schemaID;
	}

	/**
	 * Gets the number for the conglomerate.
	 *
	 * @return	A long identifier for the conglomerate
	 */
	public long	getConglomerateNumber()
	{
		return conglomerateNumber;
	}

	/**
	 * Set the conglomerate number.
	 * This is useful when swapping conglomerates, like for bulkInsert.
	 *
	 * @param conglomerateNumber	The new conglomerate number.
	 */
	public void setConglomerateNumber(long conglomerateNumber)
	{
		this.conglomerateNumber = conglomerateNumber;
	}

	/**
	 * Gets the UUID String for the conglomerate.
	 *
	 * @return	The UUID String for the conglomerate
	 */
	public UUID getUUID()
	{
		return uuid;
	}

	/**
	 * Gets the UUID for the table that the conglomerate belongs to.
	 *
	 * @return	The UUID String for the conglomerate
	 */
	public UUID	getTableID()
	{
		return	tableID;
	}

	/**
	 * Gets the UUID for the schema that the conglomerate belongs to.
	 *
	 * @return	The UUID String for the schema that the conglomerate belongs to
	 */
	public UUID	getSchemaID()
	{
		return schemaID;
	}

	/**
	 * Tells whether the conglomerate can be used as an index.
	 *
	 * @return	TRUE if the conglomerate can be used as an index, FALSE if not
	 */
	public boolean	isIndex()
	{
		return indexable;
	}

	/**
	 * Tells whether the conglomerate is an index backing up a constraint.
	 *
	 * @return	TRUE if the conglomerate is an index backing up a constraint, FALSE if not
	 */
	public boolean	isConstraint()
	{
		return forConstraint;
	}

	/**
	 * Gets the name of the conglomerate.  For heaps, this is null.  For
	 * indexes, it is the index name.
	 *
	 * @return	The name of the conglomerate, null if it's the heap for a table.
	 */
	public String getConglomerateName()
	{
		return name;
	}

	/**
	 * Set the name of the conglomerate.  Used only by rename index.
	 *
	 * @param	newName The new name of the conglomerate.
	 */
	public void	setConglomerateName(String newName)
	{
		name = newName;
	}


	/**
	 * Gets the index row generator for this conglomerate, null if the
	 * conglomerate is not an index.
	 *
	 * @return	The index descriptor for this conglomerate, if any.
	 */
	public IndexRowGenerator getIndexDescriptor()
	{
		return indexRowGenerator;
	}


	/**
	 * Set the column names for this conglomerate descriptor.
	 * This is useful for tracing the optimizer.
	 *
	 * @param columnNames	0-based array of column names.
	 */
	public void setColumnNames(String[] columnNames)
	{
		this.columnNames = columnNames;
	}

	/**
	 * Get the column names for this conglomerate descriptor.
	 * This is useful for tracing the optimizer.
	 *
	 * @return the column names for the conglomerate descriptor.
	 */
	public String[] getColumnNames()
	{
		return columnNames;
	}

	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider
	 */
	public DependableFinder getDependableFinder() 
	{
	    return	getDependableFinder(StoredFormatIds.CONGLOMERATE_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(name != null,
				"ConglomerateDescriptor only expected to be provider for indexes");
		}
		return name;
	}

	/**
	 * Get the provider's UUID
	 *
	 * @return 	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return uuid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		if (indexable)
		{
			return Dependable.INDEX;
		}
		else
		{
			return Dependable.HEAP;
		}
	}

	/**
	 * Convert the conglomerate descriptor to a String
	 *
	 * @return	The conglomerate descriptor as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			StringBuffer keyString = new StringBuffer();

			if (indexable && columnNames != null )
			{
				int[] keyColumns = indexRowGenerator.baseColumnPositions();

				keyString.append(", key columns = {").append(columnNames[keyColumns[0] - 1]);
				for (int index = 1; index < keyColumns.length; index++)
				{
					keyString.append(", ").append(columnNames[keyColumns[index] - 1]);
				}
				keyString.append("}");
			}

			return "ConglomerateDescriptor: conglomerateNumber = " + conglomerateNumber +
				" name = " + name +
				" uuid = " + uuid +
				" indexable = " + indexable + keyString.toString();
		}
		else
		{
			return "";
		}
	}

	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType()
	{
		if (indexable)
			return "Index";
		else
			return "Table";
	}

	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() { return name; }
    
    /**
     * Drop this ConglomerateDescriptor when it represents
     * an index. If this is the last desciptor for
     * a physical index then the physical index (conglomerate)
     * and its descriptor will be dropped.
     * 
     * @param lcc Connection context to use for dropping
     * @param td TableDescriptor for the table to which this
     *  conglomerate belongs
     * @return If the conglomerate described by this descriptor
     *  is an index conglomerate that is shared by multiple
     *  constraints/indexes, then we may have to create a new
     *  conglomerate to satisfy the constraints/indexes which
     *  remain after we drop the existing conglomerate.  If that's
     *  needed then we'll return a conglomerate descriptor which
     *  describes what the new conglomerate must look like.  It
     *  is then up to the caller of this method to create a new
     *  corresponding conglomerate.  We don't create the index
     *  here because depending on who called us, it might not
     *  make sense to create it--esp. if we get here because of
     *  a DROP TABLE.
     * @throws StandardException
     */
	public ConglomerateDescriptor drop(LanguageConnectionContext lcc,
		TableDescriptor td) throws StandardException
	{
        DataDictionary dd = getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();
        
        // invalidate any prepared statements that
        // depended on the index (including this one)
        dm.invalidateFor(this, DependencyManager.DROP_INDEX, lcc);
	    
        // only drop the conglomerate if no similar index but with different
	    // name. Get from dd in case we drop other dup indexes with a cascade operation	    
	    ConglomerateDescriptor [] congDescs =
	        dd.getConglomerateDescriptors(getConglomerateNumber());

		boolean dropConglom = false;
		ConglomerateDescriptor physicalCD = null;
		if (congDescs.length == 1)
			dropConglom = true;
		else
		{
		 	/* There are multiple conglomerate descriptors which share
			 * the same physical conglomerate.  That said, if we are
			 * dropping the *ONLY* conglomerate descriptor that fully
			 * matches the physical conglomerate, then we have to do
			 * a little extra work.  Namely, if the physical conglomerate
			 * is unique and this descriptor is unique, but none of the
			 * other descriptors which share with this one are unique,
			 * then we have to "update" the physical conglomerate to
			 * be non-unique. This ensures correct behavior for the
			 * remaining descriptors. (DERBY-3299)
			 *
			 * Note that "update the physical conglomerate" above is
			 * currently implemented as "drop the old conglomerate"
			 * (now) and "create a new (replacement) one" (later--let
			 * the caller do it).  Possible improvements to that logic
			 * may be desirable in the future...
			 */

			boolean needNewConglomerate;

			/* Find a conglomerate descriptor that fully describes what
			 * a physical conglomerate would have to look like in order
			 * to fulfill the requirements (esp. uniqueness) of _all_
			 * conglomerate descriptors which share a physical conglomerate
			 * with this one. "true" in the next line means that when we
			 * search for such a conglomerate, we should ignore "this"
			 * descriptor--because we're going to drop this one and we
			 * want to see what the physical conglomerate must look like
			 * when "this" descriptor does not exist.  Note that this
			 * call should never return null because we only get here
			 * if more than one descriptor shares a conglom with this
			 * one--so at the very least we'll have two descriptors,
			 * which means the following call should return the "other"
			 * one.
			 */

			physicalCD = describeSharedConglomerate(congDescs, true);
			IndexRowGenerator othersIRG = physicalCD.getIndexDescriptor();

			/* Let OTHERS denote the set of "other" descriptors which
			 * share a physical conglomerate with this one.  Recall
			 * that (for now) 1) sharing descriptors must always have
			 * the same columns referenced in the same order, and
			 * 2) if a unique descriptor shares a conglomerate with
			 * a non-unique descriptor, the physical conglomerate
			 * must itself be unique. So given that, we have four
			 * possible cases:
			 *
			 *  1. "this" is unique, none of OTHERS are unique
			 *  2. "this" is unique, 1 or more of OTHERS is unique
			 *  3. "this" is not unique, none of OTHERS are unique
			 *  4. "this" is not unique, 1 or more of OTHERS is unique
			 *
			 * In case 1 "this" conglomerate descriptor must be the
			 * _only_ one which fully matches the physical conglom.
			 * In case 4, "this" descriptor does _not_ fully match
			 * the physical conglomerate. In cases 2 and 3, "this"
			 * descriptor fully matches the physical conglom, but it
			 * is NOT the only one to do so--which means we don't need
			 * to update the physical conglomerate when we drop "this"
			 * (because OTHERS need the exact same physical conglom).
			 * The only case that actually requires an "updated"
			 * conglomerate, then, is case 1, since the physical
			 * conglomerate for the remaining descriptors no longer
			 * has a uniqueness requirement.
			 */
			needNewConglomerate =
				(indexRowGenerator.isUnique() && !othersIRG.isUnique()) ||
					(indexRowGenerator.isUniqueWithDuplicateNulls() && 
						!othersIRG.isUniqueWithDuplicateNulls());

			if (needNewConglomerate)
			{
				/* We have to create a new backing conglomerate
				 * to correctly represent the remaing (sharing)
				 * descriptors, so drop the physical conglomerate
				 * now.  The caller of the method can then create
				 * new conglomerate as/if needed.
				 */
				dropConglom = true;
			}
			else
				physicalCD = null;
		}

        /* DERBY-5681 Drop statistics */
        dd.dropStatisticsDescriptors(td.getUUID(), getUUID(), tc);

	    if (dropConglom)
	    {
	        /* Drop the physical conglomerate */
	        tc.dropConglomerate(getConglomerateNumber());
	    }

	    /* Drop the conglomerate descriptor */
	    dd.dropConglomerateDescriptor(this, tc);
	    
	    /* 
	     ** Remove the conglomerate descriptor from the list hanging off of the
	     ** table descriptor
	     */
	    td.removeConglomerateDescriptor(this);
	    return physicalCD;
	}

	/**
	 * This method searches the received array of conglom descriptors
	 * to find all descriptors that currently share a physical conglom
	 * with "this".  The method then searches within those sharing
	 * descriptors to find one that fully describes what a physical
	 * conglom would have to look like in order to support _all_ of
	 * the sharing descriptors in the array--esp. one that correctly
	 * enforces the uniqueness requirements for those descriptors.
	 *
	 * @param descriptors Array of conglomerate descriptors in
	 *  which to search; the array may include an entry for "this";
	 *  it should not be null.
	 *
	 * @param ignoreThis If true then we will NOT consider "this"
	 *  conglomerate descriptor in our search.  That is, we will
	 *  find a descriptor to describe what a physical conglomerate
	 *  would have to look like in order to support all sharing
	 *  descriptors OTHER THAN this one.
	 *
	 * @return A conglomerate descriptor, pulled from the received
	 *  array, that describes what a physical conglomerate would
	 *  have to look to like in order to support all sharing
	 *  descriptors (minus "this" if ignoreThis is true).
	 */
	public ConglomerateDescriptor describeSharedConglomerate(
		ConglomerateDescriptor [] descriptors, boolean ignoreThis)
		throws StandardException
	{
		/* Descriptor for the heap always correctly describes the
		 * physical conglomerate, as sharing of the heap is not
		 * allowed.  So if this is a heap descriptor and "descriptors"
		 * has any entries whose conglomerate number matches this
		 * descriptor's conglomerate number, then that element should
		 * be the same descriptor as "this".
		 */
		if (!isIndex())
		{
			ConglomerateDescriptor heap = null;
			for (int i = 0; i < descriptors.length; i++)
			{
				if (getConglomerateNumber() !=
					descriptors[i].getConglomerateNumber())
				{
					continue;
				}

				if (SanityManager.DEBUG)
				{
					if (!descriptors[i].getUUID().equals(getUUID()))
					{
						SanityManager.THROWASSERT(
							"Should not have multiple descriptors for " +
							"heap conglomerate " + getConglomerateNumber());
					}
				}

				heap = descriptors[i];
			}

			return heap;
		}

		/* In order to be shared by multiple conglomerate descriptors
		 * the physical conglomerate must necessarily satisfy the
		 * following criteria:
		 *
		 *  1. If any of the sharing descriptors is unique, then
		 *     the physical conglomerate must also be unique.
		 *
		 *  2. If none of sharing descriptors are unique and any of 
		 *     the descriptors are UniqueWithDuplicateNulls the physical
		 *     conglomerate must also be UniqueWithDuplicateNulls
		 *
		 *  3. If none of the sharing descriptors are unique or 
		 *     UniqueWithDuplicateNulls, the physical conglomerate 
		 *     must not be unique.
		 *
		 *  4. If the physical conglomerate has n columns, then all
		 *     sharing descriptors must have n columns, as well.
		 *
		 * These criteria follow from the "share conglom" detection logic
		 * found in CreateIndexConstantAction.executeConstantAction().
		 * See that class for details.
		 *
		 * So walk through the conglomerate descriptors that share
		 * a conglomerate with this one and see if any of them is
		 * unique.
		 */

		ConglomerateDescriptor returnDesc = null;
		for (int i = 0; i < descriptors.length; i++)
		{
			// Skip if it's not an index (i.e. it's a heap descriptor).
			if (!descriptors[i].isIndex())
				continue;

			// Skip if it doesn't share with "this".
			if (getConglomerateNumber() !=
				descriptors[i].getConglomerateNumber())
			{
				continue;
			}

			// Skip if ignoreThis is true and it describes "this".
			// DERBY-5249. We need to check both the UUID and the
			// conglomerateName to see if this is a match, because
			// databases prior to the DERBY-655 fix may have a 
			// duplicate conglomerateID
			if (ignoreThis &&
				getUUID().equals(descriptors[i].getUUID()) &&
				getConglomerateName().equals(descriptors[i].
							getConglomerateName())
				)
			{
				continue;
			}

			if (descriptors[i].getIndexDescriptor().isUnique())
			{
				/* Given criteria #1 and #4 described above, if we
				 * have a unique conglomerate descriptor then we've
				 * found what we need, so we're done.
				 */
				returnDesc = descriptors[i];
				break;
			}

			if (descriptors[i].getIndexDescriptor()
					.isUniqueWithDuplicateNulls())
			{
				/* Criteria #2. Remember this descriptor. If we don't find
				 * any unique descriptor we will use this.
				 */
				returnDesc = descriptors[i];
			}
			else if (returnDesc == null)
			{
				/* Criteria #3 If no other descriptor found satifying
				 * #1 or #2 this descriptor will be used.
				 */
				 returnDesc = descriptors[i];
			}
		}

		if (SanityManager.DEBUG)
		{
			if (returnDesc == null)
			{
				SanityManager.THROWASSERT(
					"Failed to find sharable conglomerate descriptor " +
					"for index conglomerate # " + getConglomerateNumber());
			}
		}

		return returnDesc;
	}
}
