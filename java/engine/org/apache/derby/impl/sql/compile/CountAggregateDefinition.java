/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
