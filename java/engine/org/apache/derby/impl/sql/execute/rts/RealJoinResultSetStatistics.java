/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for JoinResultSet.

  @author jerry

*/
public abstract class RealJoinResultSetStatistics 
	extends RealNoPutResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/* Leave these fields public for object inspectors */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	public long restrictionTime;


	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealJoinResultSetStatistics(
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
										double optimizerEstimatedCost
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
			optimizerEstimatedRowCount,
			optimizerEstimatedCost
			);
		this.rowsSeenLeft = rowsSeenLeft;
		this.rowsSeenRight = rowsSeenRight;
		this.rowsReturned = rowsReturned;
		this.restrictionTime = restrictionTime;
	}

 
	// Class implementation
		/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_JOIN);
  }
}
