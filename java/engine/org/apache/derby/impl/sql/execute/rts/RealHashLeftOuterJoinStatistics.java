/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;


/**
  ResultSetStatistics implemenation for HashLeftOuterJoinResultSet.

  @author jerry

*/
public class RealHashLeftOuterJoinStatistics 
	extends RealNestedLoopLeftOuterJoinStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;


	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashLeftOuterJoinStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								int rowsSeenLeft,
								int rowsSeenRight,
								int rowsReturned,
								long restrictionTime,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
								ResultSetStatistics leftResultSetStatistics,
								ResultSetStatistics rightResultSetStatistics,
								int emptyRightRowsReturned
								)
	{
		super(
			numOpens,
			rowsSeen,
			rowsFiltered,
			constructorTime,
			openTime,
			nextTime,
			closeTime,
			resultSetNumber,
			rowsSeenLeft,
			rowsSeenRight,
			rowsReturned,
			restrictionTime,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			leftResultSetStatistics,
			rightResultSetStatistics,
			emptyRightRowsReturned
			);
	}

	// ResultSetStatistics methods

	// Class implementation
	protected void setNames()
	{
		nodeName = MessageService.getTextMessage(SQLState.RTS_HASH_LEFT_OJ);
		resultSetName =
			MessageService.getTextMessage(SQLState.RTS_HASH_LEFT_OJ_RS);
	}
}
