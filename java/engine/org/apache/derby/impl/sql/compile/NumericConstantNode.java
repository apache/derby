/*

   Derby - Class org.apache.derby.impl.sql.compile.NumericConstantNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeUtilities;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.util.ReuseFactory;

import java.sql.Types;
import java.math.BigDecimal;

public final class NumericConstantNode extends ConstantNode
{
	/**
	 * Initializer for a typed null node
	 *
	 * @param arg1	The TypeId for the type of node
	 *
	 * - OR -
	 *
	 * @param arg1	An object containing the value of the constant.
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1)
		throws StandardException
	{
		int precision = 0, scal = 0, maxwidth = 0;
		Boolean isNullable;
		boolean valueInP; // value in Predicate-- if TRUE a value was passed in
		TypeId  typeId = null;
		int typeid = 0;
		Object val = null;	

		if (arg1 instanceof TypeId)
		{
			typeId = (TypeId)arg1;
			isNullable = Boolean.TRUE;
			valueInP = false;
			maxwidth = 0;
		}

		else	
		{
			isNullable = Boolean.FALSE;
			val = arg1;
			valueInP = true;
		}

		
		switch (getNodeType())
		{
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.TINYINT;
				setValue(getDataValueFactory().getDataValue(
														((Byte)val).byteValue()));
			} 
			break;

		case C_NodeTypes.INT_CONSTANT_NODE:
			precision = TypeId.INT_PRECISION;
			scal = TypeId.INT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.INT_MAXWIDTH;
				typeid = Types.INTEGER;
				setValue(
					getDataValueFactory().getDataValue(
												((Integer) val).intValue()));
			}
			break;

		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.SMALLINT;
				setValue(
					getDataValueFactory().getDataValue(
												((Short) val).shortValue()));
			}
			break;

		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			precision = TypeId.LONGINT_PRECISION;
			scal = TypeId.LONGINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.LONGINT_MAXWIDTH;
				typeid = Types.BIGINT;
				setValue(getDataValueFactory().getDataValue(((Long) arg1).longValue()));
			}
			break;
			
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			if (valueInP)
			{
				BigDecimal dVal = (BigDecimal)val;

				typeid = Types.DECIMAL;
				precision = org.apache.derby.iapi.types.SQLDecimal.getPrecision(dVal);
				scal = (dVal == null) ? 0 : dVal.scale();
				/* be consistent with our convention on maxwidth, see also
				 * exactNumericType(), otherwise we get format problem, b 3923
				 */
				maxwidth = DataTypeUtilities.computeMaxWidth( precision, scal);
				setValue(getDataValueFactory().getDataValue(dVal));
			}
			else
			{
				precision = TypeCompiler.DEFAULT_DECIMAL_PRECISION;
				scal = TypeCompiler.DEFAULT_DECIMAL_SCALE;
				maxwidth = TypeId.DECIMAL_MAXWIDTH;
			}
			break;
												   
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			precision = TypeId.DOUBLE_PRECISION;
			scal = TypeId.DOUBLE_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.DOUBLE_MAXWIDTH;
				typeid = Types.DOUBLE;
				setValue(getDataValueFactory().getDataValue(
											((Double) arg1).doubleValue()));
			}
			break;

		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			precision = TypeId.REAL_PRECISION;
			scal = TypeId.REAL_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.REAL_MAXWIDTH;
				typeid = Types.REAL;
				setValue(
					getDataValueFactory().getDataValue(
											((Float) arg1).floatValue()));
			}
			break;
			
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
								"Unexpected nodeType = " + getNodeType());
			}
			break;
		}
		
		super.init(
				   (typeId != null) ?  typeId :
				     TypeId.getBuiltInTypeId(typeid),

				   ReuseFactory.getInteger(precision), 
				   ReuseFactory.getInteger(scal), 
				   isNullable, 
				   ReuseFactory.getInteger(maxwidth));
	}
	
	long	getLong() throws StandardException
	{
		return value.getLong();
	}

	/**
	 * Return the value from this DoubleConstantNode
	 *
	 * @return	The value of this DoubleConstantNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

	double	getDouble() throws StandardException
	{
		return value.getDouble();
	}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 * @exception StandardException		Thrown on error
	 */
	Object getConstantValueAsObject()
		throws StandardException
	{
		return value.getObject();
	}

		/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 * @return		The compiled Expression, 
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		switch (getNodeType())
		{
		case C_NodeTypes.INT_CONSTANT_NODE:
			mb.push(value.getInt());
			break;
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			mb.push(value.getByte());
			break;
		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			mb.push(value.getShort());
			break;
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			mb.pushNewStart("java.math.BigDecimal");
			mb.push(value.toString());
			mb.pushNewComplete(1);
			break;
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			mb.push(getDouble());
			break;
		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			mb.push(value.getFloat());
			break;
		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			mb.push(getLong());
			break;
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
						  "Unexpected nodeType = " + getNodeType());
			}
		}	
	}
}		
