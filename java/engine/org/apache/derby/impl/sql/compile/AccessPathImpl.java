/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizer;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

class AccessPathImpl implements AccessPath
{
	ConglomerateDescriptor	cd = null;
	private CostEstimate	costEstimate = null;
	boolean					coveringIndexScan = false;
	boolean					nonMatchingIndexScan = false;
	JoinStrategy			joinStrategy = null;
	int						lockMode;
	Optimizer				optimizer;
	private String			accessPathName = "";

	AccessPathImpl(Optimizer optimizer)
	{
		this.optimizer = optimizer;
	}

	/** @see AccessPath#setConglomerateDescriptor */
	public void setConglomerateDescriptor(ConglomerateDescriptor cd)
	{
		this.cd = cd;
	}

	/** @see AccessPath#getConglomerateDescriptor */
	public ConglomerateDescriptor getConglomerateDescriptor()
	{
		return cd;
	}

	/** @see AccessPath#setCostEstimate */
	public void setCostEstimate(CostEstimate costEstimate)
	{
		/*
		** CostEstimates are mutable, so keep the best cost estimate in
		** a copy.
		*/
		if (this.costEstimate == null)
		{
			if (costEstimate != null)
			{
				this.costEstimate = costEstimate.cloneMe();
			}
		}
		else
		{
			if (costEstimate == null)
				this.costEstimate = null;
			else
				this.costEstimate.setCost(costEstimate);
		}
	}

	/** @see AccessPath#getCostEstimate */
	public CostEstimate getCostEstimate()
	{
		return costEstimate;
	}

	/** @see AccessPath#setCoveringIndexScan */
	public void setCoveringIndexScan(boolean coveringIndexScan)
	{
		this.coveringIndexScan = coveringIndexScan;
	}

	/** @see AccessPath#getCoveringIndexScan */
	public boolean getCoveringIndexScan()
	{
		return coveringIndexScan;
	}

	/** @see AccessPath#setNonMatchingIndexScan */
	public void setNonMatchingIndexScan(boolean nonMatchingIndexScan)
	{
		this.nonMatchingIndexScan = nonMatchingIndexScan;
	}

	/** @see AccessPath#getNonMatchingIndexScan */
	public boolean getNonMatchingIndexScan()
	{
		return nonMatchingIndexScan;
	}

	/** @see AccessPath#setJoinStrategy */
	public void setJoinStrategy(JoinStrategy joinStrategy)
	{
		this.joinStrategy = joinStrategy;
	}

	/** @see AccessPath#getJoinStrategy */
	public JoinStrategy getJoinStrategy()
	{
		return joinStrategy;
	}

	/** @see AccessPath#setLockMode */
	public void setLockMode(int lockMode)
	{
		this.lockMode = lockMode;
	}

	/** @see AccessPath#getLockMode */
	public int getLockMode()
	{
		return lockMode;
	}

	/** @see AccessPath#copy */
	public void copy(AccessPath copyFrom)
	{
		setConglomerateDescriptor(copyFrom.getConglomerateDescriptor());
		setCostEstimate(copyFrom.getCostEstimate());
		setCoveringIndexScan(copyFrom.getCoveringIndexScan());
		setNonMatchingIndexScan(copyFrom.getNonMatchingIndexScan());
		setJoinStrategy(copyFrom.getJoinStrategy());
		setLockMode(copyFrom.getLockMode());
	}

	/** @see AccessPath#getOptimizer */
	public Optimizer getOptimizer()
	{
		return optimizer;
	}

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "cd == " + cd +
				", costEstimate == " + costEstimate +
				", coveringIndexScan == " + coveringIndexScan +
				", nonMatchingIndexScan == " + nonMatchingIndexScan +
				", joinStrategy == " + joinStrategy +
				", lockMode == " + lockMode +
				", optimizer level == " + optimizer.getLevel();
		}
		else
		{
			return "";
		}
	}
	
	/** @see AccessPath#initializeAccessPathName */
	public void initializeAccessPathName(DataDictionary dd, TableDescriptor td)
	       throws StandardException
	{
		if (cd == null)
			return;

		if (cd.isConstraint())
		{
			ConstraintDescriptor constraintDesc = 
				dd.getConstraintDescriptor(td, cd.getUUID());
			if (constraintDesc == null)
			{
				throw StandardException.newException(
										SQLState.LANG_OBJECT_NOT_FOUND,
										"CONSTRAINT on TABLE",
										td.getName());
			}
			accessPathName = constraintDesc.getConstraintName();
		} 
		else if (cd.isIndex())
		{
			accessPathName = cd.getConglomerateName();
		} 
		else 
		{
			accessPathName = "";
		} 
	}
}
