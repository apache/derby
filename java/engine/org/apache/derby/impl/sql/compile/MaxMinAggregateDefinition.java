/*

   Derby - Class org.apache.derby.impl.sql.compile.MaxMinAggregateDefinition

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.execute.MaxMinAggregator;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.reference.ClassName;

/**
 * Defintion for the MAX()/MIN() aggregates.
 *
 * @author jamie
 */
public class MaxMinAggregateDefinition 
		implements AggregateDefinition 
{
	private boolean isMax;
  
	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
	public MaxMinAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype.  Accept NumberDataValues
	 * only.  
	 * <P>
	 * <I>Note</I>: In the future you should be able to do
	 * a sum user data types.  One option would be to run
	 * sum on anything that implements divide().  
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
		LanguageConnectionContext lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

			/*
			** MIN and MAX may return null
			*/
		DataTypeDescriptor dts = new DataTypeDescriptor((DataTypeDescriptor) inputType, true);
		TypeId compType = dts.getTypeId();

		/*
		** If the class implements NumberDataValue, then we
		** are in business.  Return type is same as input
		** type.
		*/
		if (compType.orderable(
						lcc.getLanguageConnectionFactory().getClassFactory()))
		{
			aggregatorClass.append(ClassName.MaxMinAggregator);
			
			return dts;
		}
		return null;
	}

	/**
	 * This is set by the parser.
	 */
	public final void setMaxOrMin(boolean isMax)
	{
		this.isMax = isMax;
	}

	/**
	 * Return if the aggregator class is for min/max.
	 *
	 * @return boolean true/false
	 */
	public final boolean isMax()
	{
		return(isMax);
	}
}
