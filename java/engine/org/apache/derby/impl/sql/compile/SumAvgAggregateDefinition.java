/*

   Derby - Class org.apache.derby.impl.sql.compile.SumAvgAggregateDefinition

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * Definition for the SUM()/AVG() aggregates.
 *
 */
class SumAvgAggregateDefinition
		implements AggregateDefinition 
{
	private boolean isSum;
	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
    public SumAvgAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype.  Accept NumberDataValues
	 * only.  
	 * <P>
	 * <I>Note</I>: In the future you should be able to do
	 * a sum user data types.  One option would be to run
	 * sum on anything that implements plus().  In which
	 * case avg() would need divide().
	 *
	 * @param inputType	the input type, either a user type or a java.lang object
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final DataTypeDescriptor	getAggregator(DataTypeDescriptor inputType, 
				StringBuffer aggregatorClass) 
	{
		try
		{
			TypeId compType = inputType.getTypeId();
		
			CompilerContext cc = (CompilerContext)
				QueryTreeNode.getContext(CompilerContext.CONTEXT_ID);
			TypeCompilerFactory tcf = cc.getTypeCompilerFactory();
			TypeCompiler tc = tcf.getTypeCompiler(compType);
		
			/*
			** If the class implements NumberDataValue, then we
			** are in business.  Return type is same as input
			** type.
			*/
			if (compType.isNumericTypeId())
			{
				aggregatorClass.append(getAggregatorClassName());

				DataTypeDescriptor outDts = tc.resolveArithmeticOperation( 
                        inputType, inputType, getOperator());
				/*
				** SUM and AVG may return null
				*/
				return outDts.getNullabilityType(true);
			}
		}
		catch (StandardException e)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected exception", e);
			}
		}

		return null;
	}

	/**
	 * Return the aggregator class.  
	 *
	 * @return SumAggregator.CLASS_NAME/AvgAggregator.CLASS_NAME
	 */
	private String getAggregatorClassName()
	{
		if ( isSum )
				return ClassName.SumAggregator;
		else
				return ClassName.AvgAggregator;
	}

	/**
	 * Return the arithmetic operator corresponding
	 * to this operation.
	 *
	 * @return TypeCompiler.SUM_OP /TypeCompiler.AVG_OP
	 */
	protected String getOperator()
	{
		if ( isSum )
				return TypeCompiler.SUM_OP;
		else
				return TypeCompiler.AVG_OP;
	}

	/**
	 * This is set by the parser.
	 */
    final void setSumOrAvg(boolean isSum)
	{
		this.isSum = isSum;
	}

}
