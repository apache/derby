/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;

/**
 * A NormalizeResultSetNode represents a normalization result set for any 
 * child result set that needs one.
 *
 * @author Jerry Brenner
 */

public class NormalizeResultSetNode extends SingleChildResultSetNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	 * this indicates if the normalize is being performed for an Update
	 * statement or not. The row passed to update also has
	 * before values of the columns being updated-- we need not 
	 * normalize these values. 
	 */
	private boolean forUpdate;

	/**
	 * Initializer for a NormalizeResultSetNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param rcl			The RCL for the node
	 * @param tableProperties	Properties list associated with the table
	 * @param forUpdate 	tells us if the normalize operation is being
	 * performed on behalf of an update statement. 
	 */

	public void init(
							Object childResult,
							Object rcl,
							Object tableProperties,
							Object forUpdate)
	{
		super.init(childResult, tableProperties);
		resultColumns = (ResultColumnList) rcl;
		this.forUpdate = ((Boolean)forUpdate).booleanValue();
	}


    /**
     *
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		int				erdNumber;

		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		// Generate the child ResultSet

		// Get the cost estimate for the child
		costEstimate = childResult.getFinalCostEstimate();

		erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);
		childResult.generate(acb, mb);
		acb.pushThisAsActivation(mb);
		mb.push(resultSetNumber);
		mb.push(erdNumber);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		mb.push(forUpdate);
		closeMethodArgument(acb, mb);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 8);
	}

	/**
	 * set the Information gathered from the parent table that is 
	 * required to peform a referential action on dependent table.
	 *
	 * @return Nothing.
	 */
	public void setRefActionInfo(long fkIndexConglomId, 
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		childResult.setRefActionInfo(fkIndexConglomId,
								   fkColArray,
								   parentResultSetId,
								   dependentScan);
	}


}
