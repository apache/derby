/*

   Derby - Class org.apache.derby.impl.store.access.btree.SearchParameters

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

package org.apache.derby.impl.store.access.btree;

import java.io.PrintStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.types.DataValueDescriptor;


/**

  Parameters that are passed down during a recursive b-tree search.
  This class is intended to be used as a struct, primarily to make
  it easier to pass a number of search parameters around, and also
  to make it easy to re-use objects and not re-allocate.

**/

public class SearchParameters
{

    /**
     * Position on key just left of a sequence of partial key matches.
     * Used by scan which will then start scan on next key.
     **/
    public static final int POSITION_LEFT_OF_PARTIAL_KEY_MATCH  = 1;

    /**
     * Position on last key in a sequence of partial key matches.
     * Used by scan which will then start scan on next key.
     **/
    public static final int POSITION_RIGHT_OF_PARTIAL_KEY_MATCH = -1;

	/**
	The key being searched for.  Never intended to be modified
	for the lifetime of the object.
	**/
	public DataValueDescriptor[] searchKey;

	/**
    Value to return in comparisons where partial key matches exactly 
    the partial key of a row.  Use this parameter to control where 
    in a duplicate partial key list to position the search.  

    Here are some examples:

    Assume: dataset of {1,0}, {5,1}, {5,2}, {6,4}; and partial key of {5}.


    If the scan is GE , then the scan intially wants to position
    on {1,0} (one before first qualifying row) - In this case when a partial
    match is found we want to return 1 when we hit {5,1}; but if the
    searchOperator is GT, then we want to return -1 on {5,1}, {5,2}, and then
    return 1 on {6,4}.


    partial_key_match_op =  POSITION_LEFT_OF_PARTIAL_KEY_MATCH: 
    Scan is looking for GE the partial key, so position the scan to the
    left of any partial key that exactly matches the partial key.
    If the scan is GE , then the scan intially wants to position
    on {1,0} (one before first qualifying row) - In this case when a partial
    match is found we want to return 1 when we hit {5,1}.

    partial_key_match_op = POSITION_RIGHT_OF_PARTIAL_KEY_MATCH: 
    Scan is looking for GT the partial key, so position the scan to the
    right of any partial key that exactly matches the partial key.
    If the scan is GT, then the scan intially wants to position
    on {5,2} (one before first qualifying row) - In this case when a partial
    match is found we want to return -1 when we hit on {5,1}, {5,2}, and then
    return 1 on {6,4}.

    partial_key_match_op =  0: 
    Scan does not care where in a set of duplicate partial keys to position 
    to (not used currently).
	**/
    int partial_key_match_op;

	/**
	An index row with the correct types for the index,
	into which rows are read during the search.
	Rows are read into the template during a page search, but
	they will be overwritten; there is only one template per
	search.
	**/
	public DataValueDescriptor[] template;

	/**
	The b-tree this search is for.  Effectively read-only for the
	lifetime of this object.
	**/
	public OpenBTree btree;

	/**
	The resulting slot from the search.  Updated when the search completes.
	**/
	public int resultSlot;

	/**
	Whether the row found exactly matched the searchKey.  Updated 
	when the search completes.
	**/
	public boolean resultExact;

	/**
	Whether the search is for the optimizer, to determine range of scan.
	**/
	public boolean searchForOptimizer;

	/**
	If this is an optimizer search, the fraction of rows that are left of 
    the current search.  When the search completes this number multiplied by
    the number of leaf rows in the table is the number of rows left of
    the result slot in the search.
	**/
	public float left_fraction;

	/**
	If this is an optimizer search, the fraction of rows that are "in" the 
    the current search.  This number is used as we descend down the tree to 
    track the percentage of rows that we think are in the current subtree
    defined by all leaf's that can be reached from the current branch.
	**/
	public float current_fraction;

	/**
	Construct search parameters.

    @exception StandardException Standard exception policy.
	**/
	public SearchParameters(
    DataValueDescriptor[]   searchKey, 
    int                     partial_key_match_op,
    DataValueDescriptor[]   template, 
    OpenBTree               btree,
    boolean                 searchForOptimizer)
        throws StandardException
	{
		this.searchKey              = searchKey;
		this.partial_key_match_op   = partial_key_match_op;
		this.template               = template;
		this.btree                  = btree;
		this.resultSlot             = 0;
		this.resultExact            = false;
		this.searchForOptimizer     = searchForOptimizer;

        if (this.searchForOptimizer)
        {
            this.left_fraction = 0;
            this.current_fraction = 1;
        }

        if (SanityManager.DEBUG)
        {
            // RESOLVE - this is ugly but has caught some problems.
            SanityManager.ASSERT(partial_key_match_op == -1||
                                 partial_key_match_op == 1);
        }
	}

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            String string = 
                "key = "      +   RowUtil.toString(searchKey)      + ";" +
                "op = "       +   (partial_key_match_op ==  1 ? "GE" :
                                  (partial_key_match_op == -1 ? "GT" :
                                   "BAD OP:" + partial_key_match_op)) + ";" +
                "template = " +   RowUtil.toString(template)        + ";" +
                // RESOLVE (mikem) - do we want to print out btree?
                // "btree = " +   btree           + ";" +
                "Slot = "     +   resultSlot      + ";" +
                "Exact = "    +   resultExact     + ";";

            return(string);
        }
        else
        {
            return(null);
        }
    }
}
