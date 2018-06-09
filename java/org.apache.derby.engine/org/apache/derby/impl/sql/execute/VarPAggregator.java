/*

   Derby - Class org.apache.derby.impl.sql.execute.VarPAggregator

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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.agg.Aggregator;

/**
 * <p>
 * This class implements the SQL Standard VAR_POP() aggregator,
 * computing a population's variance.  It uses the IBM formula described
 * <a href="http://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcolvar.htm">here</a>:
 * </p>
 *
 * <blockquote><pre><b>
 * sum(x<sub>i</sub><sup>2</sup>)/n - m<sup>2</sup>
 * 
 * where
 * 
 * n is the number of items in the population
 * m is the population average
 * x<sub>1</sub> ... x<sub>n</sub> are the items in the population
 * </b></pre></blockquote>
 *
 * <p>
 * The IBM formula can be computed without buffering up an arbitrarily
 * long list of items. The IBM formula is algebraically equivalent
 * to the textbook formula for population variance:
 * </p>
 *
 * <blockquote><pre><b>
 * sum( (x<sub>i</sub> - m)<sup>2</sup> )/n
 * </b></pre></blockquote>
 */
public class VarPAggregator<V extends Number> implements Aggregator<V, Double, VarPAggregator<V>>, Externalizable {

	private static final long serialVersionUID = 239794626052067761L;

	public static class Sums {
		double x = 0;
		double x2 = 0;		
	}
	
	protected Sums sums;
	protected int count;
	
	@Override
	public void init() {
		this.sums = new Sums();
		this.count = 0;
	}

	@Override
	public void accumulate(V value) {
		double itemd = value.doubleValue();
		sums.x += itemd;
		sums.x2 += Math.pow(itemd,2);
        count++;
	}

	@Override
	public void merge(VarPAggregator<V> otherAggregator) {
		this.sums.x += otherAggregator.sums.x;
		this.sums.x2 += otherAggregator.sums.x2;
		this.count += otherAggregator.count;
	}

	protected Double computeVar() {
		if (count == 0) return null;
		// See IBM formula: http://www-01.ibm.com/support/knowledgecenter/ssw_ibm_i_71/db2/rbafzcolvar.htm
		return (sums.x2 / count) - Math.pow(sums.x / count, 2.0); 
	}

	@Override
	public Double terminate() {
		return computeVar();
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.count = in.readInt();
		this.sums = (Sums) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(this.count);
		out.writeObject(this.sums);
	}

}
