/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;

/**
 
 <P>
 This interface is used in the column SYS.SYSSTATISTICS.STATISTICS. It
 encapsulates information collected by the UPDATE STATISTICS command
 and is used internally by the Cloudscape optimizer to estimate cost 
 and selectivity of different query plans.
 <p>
*/

public interface Statistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
	/**
	 * @return the selectivity for a set of predicates.
	 */
	double selectivity(Object[] predicates);
}
