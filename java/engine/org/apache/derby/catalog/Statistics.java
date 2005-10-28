/*

   Derby - Class org.apache.derby.catalog.Statistics

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
	 * @return the selectivity for a set of predicates.
	 */
	double selectivity(Object[] predicates);
}
