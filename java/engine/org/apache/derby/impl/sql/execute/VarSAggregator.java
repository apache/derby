/*

   Derby - Class org.apache.derby.impl.sql.execute.VarSAggregator

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

/**
 * <p>
 * This class implements the SQL Standard VAR_SAMP() aggregator,
 * computing the variance over a sample.  It uses the IBM formula described
 * <a href="http://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcolvarsamp.htm">here</a>:
 * </p>
 *
 * <blockquote><pre><b>
 * [ sum(x<sub>i</sub><sup>2</sup>) - sum(x<sub>i</sub>)<sup>2</sup>/n ]/(n-1)
 * 
 * where
 * 
 * n is the number of items in the population
 * x<sub>1</sub> ... x<sub>n</sub> are the items in the population
 * </b></pre></blockquote>
 */
public class VarSAggregator<V extends Number> extends VarPAggregator<V> {

	private static final long serialVersionUID = -741087542836440595L;

	@Override
	protected Double computeVar() {
		if (count <= 1) return null;
		// See IBM Forumula:  http://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcolvarsamp.htm
		return (sums.x2 - (Math.pow(sums.x, 2) / count)) / (count - 1);
	}
}
