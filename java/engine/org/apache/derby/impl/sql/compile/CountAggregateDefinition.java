/*

   Derby - Class org.apache.derby.impl.sql.compile.CountAggregateDefinition

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * Definition for the COUNT()/COUNT(*) aggregates.
 *
 */
class CountAggregateDefinition
		implements AggregateDefinition 
{
	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
    public CountAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype. We can run
	 * count() on anything, and it always returns a
	 * INTEGER (java.lang.Integer).
	 *
	 * @param inputType the input type, either a user type or a java.lang object
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final DataTypeDescriptor	getAggregator(DataTypeDescriptor inputType,
				StringBuffer aggregatorClass) 
	{
		aggregatorClass.append( ClassName.CountAggregator);
		/*
		** COUNT never returns NULL
		*/
		return DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.INTEGER, false);
	}

}
