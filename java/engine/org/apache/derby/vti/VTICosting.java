/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.vti
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.vti;

import java.sql.SQLException;

/**
  *	VTICosting is the interface that the query optimizer uses
  * to cost VTIs.
  * 
  The methods on the interface provide the optimizer
  * with the following information:
  <UL> 
  <LI> the estimated number of rows returned by the VTI in a single instantiation.
  <LI> the estimated cost to instantiate and iterate through the VTI.
  <LI> whether or not the VTI can be instantiated multiple times within a single query execution
  </UL>
  * <P>
  * This class can only be used within an SQL-J statement.  Using the methods
  * in application-side Java code results in Exceptions being thrown.
  * <P>
  * <I>IBM Corp reserves the right to change, rename, or
  * remove this interface at any time.</I>
  * @see org.apache.derby.vti.VTIEnvironment
 */
public interface VTICosting
{
	/**
	 * A useful constant: the default estimated number of rows returned by a VTI.
	 */
	public static final double defaultEstimatedRowCount		= 10000d;
	/**
	   A useful constant: The default estimated cost of instantiating and iterating throught a VTI.
	 */
	public static final double defaultEstimatedCost			= 100000d;

	/**
	 *  Get the estimated row count for a single scan of a VTI.
	 *
	 *  @param vtiEnvironment The VTIEnvironment.
	 *
	 *  @return	The estimated row count for a single scan of a VTI.
	 *
	 *  @exception SQLException thrown if the costing fails.
	 */
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
		throws SQLException;

	/**
	 *  Get the estimated cost for a single instantiation of a VTI.
	 *
	 *  @param vtiEnvironment The VTIEnvironment.
	 *
	 *  @return	The estimated cost for a single instantiation of a VTI.
	 *
	 *  @exception SQLException thrown if the costing fails.
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
		throws SQLException;

	/**
		 Find out if the ResultSet of the VTI can be instantiated multiple times.

		 @param vtiEnvironment The VTIEnvironment.

		 @return	True if the ResultSet can be instantiated multiple times, false if
		 can only be instantiated once.

		 @exception SQLException thrown if the costing fails.
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
		throws SQLException;
}
