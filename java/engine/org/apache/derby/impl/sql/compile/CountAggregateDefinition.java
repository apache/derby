/*

   Derby - Class org.apache.derby.impl.sql.compile.CountAggregateDefinition

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.impl.sql.execute.CountAggregator;

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.reference.ClassName;


/**
 * Defintion for the COUNT()/COUNT(*) aggregates.
 *
 * @author jamie
 */
public class CountAggregateDefinition 
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
	 * @param the input type, either a user type or a java.lang object
	 * @param implementsInterface	the interface it implements
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final TypeDescriptor	getAggregator(TypeDescriptor inputType,
				StringBuffer aggregatorClass) 
	{
		aggregatorClass.append( ClassName.CountAggregator);
		/*
		** COUNT never returns NULL
		*/
		return DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.INTEGER, false);
	}

}
