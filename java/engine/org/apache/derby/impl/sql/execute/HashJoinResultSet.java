/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;


/**
 * Hash join of 2 arbitrary result sets.
 * Simple subclass of nested loop, differentiated
 * to ease RunTimeStatistics output generation.
 */
public class HashJoinResultSet extends NestedLoopJoinResultSet
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
    public HashJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod restriction,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
    {
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
			  activation, restriction, resultSetNumber, 
			  oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, 
			  optimizerEstimatedCost, closeCleanup);
    }
}
