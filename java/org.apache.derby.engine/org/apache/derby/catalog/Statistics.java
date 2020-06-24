/*

   Derby - Class org.apache.derby.catalog.Statistics

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

package org.apache.derby.catalog;

/**
 
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
 <p>
 This interface is used in the column SYS.SYSSTATISTICS.STATISTICS. It
 encapsulates information collected by the UPDATE STATISTICS command
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
 and is used internally by the Derby optimizer to estimate cost 
 and selectivity of different query plans.
 </p>
*/

public interface Statistics
{
    /**
     * Returns the estimated number of rows in the index.
     *
     * @return Number of rows.
     */
    long getRowEstimate();
//IC see: https://issues.apache.org/jira/browse/DERBY-4938

	/**
     * @param predicates The predicates to evaluate
	 * @return the selectivity for a set of predicates.
	 */
	double selectivity(Object[] predicates);
}
