/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_StoreCostResult

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.store;

import org.apache.derby.iapi.store.access.*;

/**

Manage the result information from a single call to 
StoreCostController.getScanCost().
<p>
@see StoreCostController

**/

public class T_StoreCostResult implements StoreCostResult
{
    long    row_count;
    double  cost;

    /**
     * Get the estimated row count.
     **/
    public long getEstimatedRowCount()
    {
        return(row_count);
    }

    /**
     * Set the estimated row count.
     **/
    public void setEstimatedRowCount(
    long count)
    {
        row_count = count;
    }

    /**
     * Get the estimated cost.
     **/
    public double getEstimatedCost()
    {
        return(cost);
    }

    /**
     * Set the estimated cost.
     **/
    public void setEstimatedCost(
    double input_cost)
    {
        this.cost = input_cost;
    }

    public String toString()
    {
        return("(row count = " + row_count + ", cost = " + cost + ")");
    }
}
