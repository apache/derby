/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;
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
