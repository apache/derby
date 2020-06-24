/*

   Derby - Class org.apache.derby.iapi.types.Orderable

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

package org.apache.derby.iapi.types;

import org.apache.derby.shared.common.error.StandardException;

/** 

  The Orderable interface represents a value that can
  be linearly ordered.
  <P>
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
  Currently only supports linear (&lt;, =, &lt;=) operations.
  Eventually we may want to do other types of orderings,
  in which case there would probably be a number of interfaces
  for each "class" of ordering.
  <P>
  The implementation must handle the comparison of null
  values.  This may require some changes to the interface,
  since (at least in some contexts) comparing a value with
  null should return unknown instead of true or false.

**/

public interface Orderable
{

	/**	 Ordering operation constant representing '&lt;' **/
	static final int ORDER_OP_LESSTHAN = 1;
	/**	 Ordering operation constant representing '=' **/
	static final int ORDER_OP_EQUALS = 2;
	/**	 Ordering operation constant representing '&lt;=' **/
	static final int ORDER_OP_LESSOREQUALS = 3;

	/** 
	 * These 2 ordering operations are used by the language layer
	 * when flipping the operation due to type precedence rules.
	 * (For example, 1 &lt; 1.1 -&gt; 1.1 &gt; 1)
	 */
	/**	 Ordering operation constant representing '&gt;' **/
	static final int ORDER_OP_GREATERTHAN = 4;
	/**	 Ordering operation constant representing '&gt;=' **/
	static final int ORDER_OP_GREATEROREQUALS = 5;


}
