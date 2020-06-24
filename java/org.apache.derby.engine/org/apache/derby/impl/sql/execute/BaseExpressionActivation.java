/*

   Derby - Class org.apache.derby.impl.sql.execute.BaseExpressionActivation

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

import org.apache.derby.catalog.types.UserDefinedTypeIdImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * BaseExpressionActivation
 *
 * Support needed by Expression evaluators (Filters) and by
 * ResultSet materializers (Activations)
 */
public abstract class BaseExpressionActivation
{

	
	//
	// constructors
	//
	BaseExpressionActivation()
	{
		super();
	}


	/**
	 * <p>
	 * Get the minimum value of 4 input values.  If less than 4 values, input
	 * {@code null} for the unused parameters and place them at the end.
	 * If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * </p>
	 * 
	 * <p>
	 * If all the input values are SQL NULL, return SQL NULL. Otherwise, return
	 * the minimum value of the non-NULL inputs.
	 * </p>
	 *
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 * @param judgePrecision		precision of the judge
	 * @param judgeScale		    scale of the judge
	 * @param judgeIsNullable		nullability of the judge
	 * @param judgeMaximumWidth		maximum width of the judge
	 * @param judgeCollationType	collation type of the judge
	 * @param judgeCollationDerivation		collation derivation of the judge
	 *
	 * @return	The minimum value of the 4. 
	 */
	public static DataValueDescriptor minValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId,
											  int judgePrecision,
											  int judgeScale,
											  boolean judgeIsNullable,
											  int judgeMaximumWidth,
											  int judgeCollationType,
											  int judgeCollationDerivation)
										throws StandardException
	{
		DataValueDescriptor judge;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        if (judgeUserJDBCTypeId == -1) {
        	judge = new DataTypeDescriptor(
        			new TypeId(judgeTypeFormatId, null),
        			judgePrecision,judgeScale,judgeIsNullable,
        			judgeMaximumWidth,judgeCollationType,
        			judgeCollationDerivation).getNull();
        } else {
            judge = new TypeId(judgeTypeFormatId,
                               new UserDefinedTypeIdImpl()).getNull();
        }
			
		DataValueDescriptor minVal = v1;
		if (v2 != null &&
				(minVal.isNull() || judge.lessThan(v2, minVal).equals(true)))
			minVal = v2;
		if (v3 != null &&
				(minVal.isNull() || judge.lessThan(v3, minVal).equals(true)))
			minVal = v3;
		if (v4 != null &&
				(minVal.isNull() || judge.lessThan(v4, minVal).equals(true)))
			minVal = v4;
		return minVal;
	}


	/**
	 * <p>
	 * Get the maximum value of 4 input values.  If less than 4 values, input
	 * {@code null} for the unused parameters and place them at the end.
	 * If more than 4 input values, call this multiple times to
	 * accumulate results.  Also have judge's type as parameter to have a base
	 * upon which the comparison is based.  An example use is for code 
	 * generation in bug 3858.
	 * </p>
	 * 
	 * <p>
	 * If all the input values are SQL NULL, return SQL NULL. Otherwise, return
	 * the maximum value of the non-NULL inputs.
	 * </p>
	 *
	 * @param v1		1st value
	 * @param v2		2nd value
	 * @param v3		3rd value
	 * @param v4		4th value
	 * @param judgeTypeFormatId		type format id of the judge
	 * @param judgeUserJDBCTypeId	JDBC type id if judge is user type;
	 *								-1 if not user type
	 *
	 * @return	The maximum value of the 4.
	 */
	public static DataValueDescriptor maxValue(DataValueDescriptor v1,
											  DataValueDescriptor v2,
											  DataValueDescriptor v3,
											  DataValueDescriptor v4,
											  int judgeTypeFormatId,
											  int judgeUserJDBCTypeId,
											  int judgePrecision,
											  int judgeScale,
											  boolean judgeIsNullable,
											  int judgeMaximumWidth,
											  int judgeCollationType,
											  int judgeCollationDerivation)
										throws StandardException
	{
		DataValueDescriptor judge;
		if (judgeUserJDBCTypeId == -1) {
        	judge = new DataTypeDescriptor(
        			new TypeId(judgeTypeFormatId, null),
        			judgePrecision,judgeScale,judgeIsNullable,
        			judgeMaximumWidth,judgeCollationType,
        			judgeCollationDerivation).getNull();
		} else
			judge =  new TypeId(judgeTypeFormatId, new UserDefinedTypeIdImpl()).getNull();

		DataValueDescriptor maxVal = v1;
		if (v2 != null &&
				(maxVal.isNull() || judge.greaterThan(v2, maxVal).equals(true)))
			maxVal = v2;
		if (v3 != null &&
				(maxVal.isNull() || judge.greaterThan(v3, maxVal).equals(true)))
			maxVal = v3;
		if (v4 != null &&
				(maxVal.isNull() || judge.greaterThan(v4, maxVal).equals(true)))
			maxVal = v4;
		return maxVal;
	}

}
