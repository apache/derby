/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

/**

Manage the result information from a single call to 
StoreCostController.getScanCost().
<p>
@see StoreCostController

**/

public interface StoreCostResult
{
    /**
     * Get the estimated row count.
     **/
    public long getEstimatedRowCount();

    /**
     * Set the estimated row count.
     **/
    public void setEstimatedRowCount(long count);

    /**
     * Get the estimated cost.
     **/
    public double getEstimatedCost();

    /**
     * Set the estimated cost.
     **/
    public void setEstimatedCost(double cost);
}
