/*

   Derby - Class org.apache.derby.iapi.sql.compile.AccessPath

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.error.StandardException;

/**
 * AccessPath represents a proposed access path for an Optimizable.
 * An Optimizable may have more than one proposed AccessPath.
 */

public interface AccessPath {
	/**
	 * Set the conglomerate descriptor for this access path.
	 *
	 * @param cd	A ConglomerateDescriptor
	 *
	 * @return Nothing.
	 */
	void setConglomerateDescriptor(ConglomerateDescriptor cd);

	/**
	 * Get whatever was last set as the conglomerate descriptor.
	 * Returns null if nothing was set since the last call to startOptimizing()
	 */
	ConglomerateDescriptor getConglomerateDescriptor();

	/**
	 * Set the given cost estimate in this AccessPath.  Generally, this will
	 * be the CostEstimate for the plan currently under consideration.
	 */
	public void setCostEstimate(CostEstimate costEstimate);

	/**
	 * Get the cost estimate for this AccessPath.  This is the last one
	 * set by setCostEstimate.
	 */
	public CostEstimate getCostEstimate();

	/**
	 * Set whether or not to consider a covering index scan on the optimizable.
	 *
	 * @return Nothing.
	 */
	public void setCoveringIndexScan(boolean coveringIndexScan);

	/**
	 * Return whether or not the optimizer is considering a covering index
	 * scan on this AccessPath. 
	 *
	 * @return boolean		Whether or not the optimizer chose a covering
	 *						index scan.
	 */
	public boolean getCoveringIndexScan();

	/**
	 * Set whether or not to consider a non-matching index scan on this
	 * AccessPath. 
	 *
	 * @return Nothing.
	 */
	public void setNonMatchingIndexScan(boolean nonMatchingIndexScan);

	/**
	 * Return whether or not the optimizer is considering a non-matching
	 * index scan on this AccessPath. We expect to call this during
	 * generation, after access path selection is complete.
	 *
	 * @return boolean		Whether or not the optimizer is considering
	 *						a non-matching index scan.
	 */
	public boolean getNonMatchingIndexScan();

	/**
	 * Remember the given join strategy
	 *
	 * @param joinStrategy	The best join strategy
	 */
	public void setJoinStrategy(JoinStrategy joinStrategy);

	/**
	 * Get the join strategy, as set by setJoinStrategy().
	 */
	public JoinStrategy getJoinStrategy();

	/**
	 * Set the lock mode
	 */
	public void setLockMode(int lockMode);

	/**
	 * Get the lock mode, as last set in setLockMode().
	 */
	public int getLockMode();

	/**
	 * Copy all information from the given AccessPath to this one.
	 */
	public void copy(AccessPath copyFrom);

	/**
	 * Get the optimizer associated with this access path.
	 *
	 * @return	The optimizer associated with this access path.
	 */
	public Optimizer getOptimizer();
	
	/**
	 * Sets the "name" of the access path. if the access path represents an
	 * index then set the name to the name of the index. if it is an index
	 * created for a constraint, use the constraint name. This is called only
	 * for base tables.
	 * 
	 * @param 	td		TableDescriptor of the base table.
	 * @param 	dd		Datadictionary.
	 *
	 * @exception StandardException 	on error.
	 */
	public void initializeAccessPathName(DataDictionary dd, TableDescriptor td)
		throws StandardException;
}	
