/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.execute.SumAggregator;
import org.apache.derby.impl.sql.execute.AvgAggregator;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;

/**
 * Defintion for the SUM()/AVG() aggregates.
 *
 * @author jamie
 */
public class SumAvgAggregateDefinition
		implements AggregateDefinition 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
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
	 * @param the input type, either a user type or a java.lang object
	 * @param implementsInterface	the interface it implements
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final TypeDescriptor	getAggregator(TypeDescriptor inputType, 
				StringBuffer aggregatorClass) 
	{
		try
		{
			LanguageConnectionContext lcc = (LanguageConnectionContext)
				ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

			DataTypeDescriptor dts = new DataTypeDescriptor( (DataTypeDescriptor)inputType, inputType.isNullable());
			TypeId compType = dts.getTypeId();
		
			CompilerContext cc = (CompilerContext)
				ContextService.getContext(CompilerContext.CONTEXT_ID);
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
															dts, dts, getOperator());
				/*
				** SUM and AVG may return null
				*/
				outDts.setNullability(true);
				return outDts;
			}
		}
		catch (StandardException e)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected exception " + e);
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
	public final void setSumOrAvg(boolean isSum)
	{
		this.isSum = isSum;
	}

}
