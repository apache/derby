/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.IndexLister

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import java.util.Enumeration;
import java.util.Vector;

/**
 * This interface gathers up some tasty information about the indices on a
 * table from the DataDictionary.
 *
 */
public class IndexLister
{
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	private	TableDescriptor		tableDescriptor;
	private	IndexRowGenerator[]	indexRowGenerators;
	private	long[]				indexConglomerateNumbers;
	private String[]			indexNames;
	// the following 3 are the compact arrays, without duplicate indexes
	private IndexRowGenerator[]	distinctIndexRowGenerators;
	private	long[]				distinctIndexConglomerateNumbers;
	private String[]			distinctIndexNames;

	////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Make an IndexLister
	  *
	  *	@param	tableDescriptor	Describes the table in question.
	  *
	  */
	public	IndexLister( TableDescriptor	tableDescriptor )
	{
		this.tableDescriptor = tableDescriptor;
	}


	////////////////////////////////////////////////////////////////////////
	//
	//	INDEXLISTER METHODS
	//
	////////////////////////////////////////////////////////////////////////

    /**
	  *	Returns an array of all the index row generators on a table.
	  *
	  *	@return	an array of index row generators
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	IndexRowGenerator[]		getIndexRowGenerators()
					throws StandardException
	{
		if ( indexRowGenerators == null ) { getAllIndexes(); }
		return	indexRowGenerators;
	}

    /**
	  *	Returns an array of all the index conglomerate ids on a table.
	  *
	  *	@return	an array of index conglomerate ids
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	long[]		getIndexConglomerateNumbers()
					throws StandardException
	{
		if ( indexConglomerateNumbers == null ) { getAllIndexes(); }
		return	indexConglomerateNumbers;
	}

    /**
	  *	Returns an array of all the index names on a table.
	  *
	  *	@return	an array of index names
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	String[]		getIndexNames()	throws StandardException
	{
		if ( indexNames == null ) { getAllIndexes(); }
		return	indexNames;
	}

    /**
	  *	Returns an array of distinct index row generators on a table,
	  * erasing entries for duplicate indexes (which share same conglomerate).
	  *
	  *	@return	an array of index row generators
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	IndexRowGenerator[]		getDistinctIndexRowGenerators()
					throws StandardException
	{
		if ( distinctIndexRowGenerators == null ) { getAllIndexes(); }
		return	distinctIndexRowGenerators;
	}

    /**
	  *	Returns an array of distinct index conglomerate ids on a table.
	  * erasing entries for duplicate indexes (which share same conglomerate).
	  *
	  *	@return	an array of index conglomerate ids
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	long[]		getDistinctIndexConglomerateNumbers()
					throws StandardException
	{
		if ( distinctIndexConglomerateNumbers == null ) { getAllIndexes(); }
		return	distinctIndexConglomerateNumbers;
	}

    /**
	  *	Returns an array of index names for all distinct indexes on a table.
	  * erasing entries for duplicate indexes (which share same conglomerate).
	  *
	  *	@return	an array of index names
	  *
	  * @exception StandardException		Thrown on error
	  */
    public	String[]		getDistinctIndexNames()	throws StandardException
	{
		if ( indexNames == null ) { getAllIndexes(); }
		return	indexNames;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	MINIONS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Reads all the indices on the table and populates arrays with the
	  *	corresponding index row generators and index conglomerate ids.
	  *
	  *
	  * @exception StandardException		Thrown on error
	  */
	private void getAllIndexes()
					throws StandardException
	{
		int			indexCount = 0;

		ConglomerateDescriptor[] cds = 
			              tableDescriptor.getConglomerateDescriptors();

		/* from one end of work space, we record distinct conglomerate
		 * numbers for comparison while we iterate; from the other end of
		 * work space, we record duplicate indexes' indexes in "cds" array,
		 * so that we can skip them in later round.
		 */
		long[] workSpace = new long[cds.length - 1];  // 1 heap
		int distinctIndexCount = 0, duplicateIndex = workSpace.length - 1;

		for (int i = 0; i < cds.length; i++)
		{
			// first count the number of indices.
			ConglomerateDescriptor cd = cds[i];

			if ( ! cd.isIndex())
				continue;

			int k;
			long thisCongNum = cd.getConglomerateNumber();

			for (k = 0; k < distinctIndexCount; k++)
			{
				if (workSpace[k] == thisCongNum)
				{
					workSpace[duplicateIndex--] = i;
					break;
				}
			}
			if (k == distinctIndexCount)			// first appearence
				workSpace[distinctIndexCount++] = thisCongNum;

			indexCount++;
		}

		indexRowGenerators = new IndexRowGenerator[ indexCount ];
		indexConglomerateNumbers = new long[ indexCount ];
		indexNames = new String[ indexCount ];
		distinctIndexRowGenerators = new IndexRowGenerator[ distinctIndexCount ];
		distinctIndexConglomerateNumbers = new long[ distinctIndexCount ];
		distinctIndexNames = new String[ distinctIndexCount ];

		int duplicatePtr = workSpace.length - 1;
		for ( int i = 0, j = -1, k = -1; i < cds.length; i++ )
		{
			ConglomerateDescriptor cd = cds[i];

			if ( ! cd.isIndex())
				continue;

			indexRowGenerators[++j] = 
				(IndexRowGenerator)cd.getIndexDescriptor();
			indexConglomerateNumbers[j] = cd.getConglomerateNumber();
			if (!(cd.isConstraint()))
			{
				// only fill index name if it is not a constraint.
				indexNames[j] = cd.getConglomerateName();
			}

			if (duplicatePtr > duplicateIndex && i == (int) workSpace[duplicatePtr])
				duplicatePtr--;
			else
			{
				distinctIndexRowGenerators[++k] = indexRowGenerators[j];
				distinctIndexConglomerateNumbers[k] = indexConglomerateNumbers[j];
				distinctIndexNames[k] = indexNames[j];
			}
		}
	}
}
