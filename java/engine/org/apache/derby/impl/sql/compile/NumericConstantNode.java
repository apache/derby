/*

   Derby - Class org.apache.derby.impl.sql.compile.NumericConstantNode

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

package	org.apache.derby.impl.sql.compile;

import java.sql.Types;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.DataTypeUtilities;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLTinyint;
import org.apache.derby.iapi.types.TypeId;

public final class NumericConstantNode extends ConstantNode
{
    /**
     * Constructor for a typed null node
     * @param t type
     * @param cm context manager
     * @throws StandardException
     */
    NumericConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(cm);
        setType(t,
                getPrecision(t, null),
                getScale(t, null),
                true,
                getMaxWidth(t, null));
        setNodeType(getNodeType(t));
    }

    /**
     * @param value An object containing the value of the constant.
     * @param cm context manager
     * @throws StandardException
     */
    NumericConstantNode(TypeId t, Object value, ContextManager cm)
            throws StandardException {
        super(cm);
        setNodeType(getNodeType(t));
        setType(t,
                getPrecision(t, value),
                getScale(t, value),
                false,
                getMaxWidth(t, value));
        setValue(t, value);
    }

    private int getPrecision(TypeId t, Object val) throws StandardException {
        int foo = t.getJDBCTypeId();
       //switch (t.getJDBCTypeId()) {
        switch (foo) {
       case Types.TINYINT:
           return TypeId.SMALLINT_PRECISION; // FIXME
       case Types.INTEGER:
           return TypeId.INT_PRECISION;
       case Types.SMALLINT:
           return TypeId.SMALLINT_PRECISION;
       case Types.BIGINT:
           return TypeId.LONGINT_PRECISION;
       case Types.DECIMAL:
            if (val != null) {
               NumberDataValue constantDecimal =
                    getDataValueFactory().getDecimalDataValue((String)val);
               return constantDecimal.getDecimalValuePrecision();
            } else {
                return TypeId.DECIMAL_PRECISION;
            }
       case Types.DOUBLE:
           return TypeId.DOUBLE_PRECISION;
       case Types.REAL:
           return TypeId.REAL_PRECISION;
       default:
           if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
           }
           return 0;
		}
    }

    private int getScale(TypeId t, Object val) throws StandardException {
       switch (t.getJDBCTypeId()) {
       case Types.TINYINT:
           return TypeId.SMALLINT_SCALE; // FIXME
       case Types.INTEGER:
           return TypeId.INT_SCALE;
       case Types.SMALLINT:
           return TypeId.SMALLINT_SCALE;
       case Types.BIGINT:
           return TypeId.LONGINT_SCALE;
       case Types.DECIMAL:
            if (val != null) {
               NumberDataValue constantDecimal =
                        getDataValueFactory().getDecimalDataValue((String)val);
               return constantDecimal.getDecimalValueScale();
            } else {
                return TypeId.DECIMAL_SCALE;
            }
       case Types.DOUBLE:
            return TypeId.DOUBLE_SCALE;
       case Types.REAL:
           return TypeId.REAL_SCALE;
       default:
           if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
           }
           return 0;
        }
    }

    private int getMaxWidth(TypeId t, Object val) throws StandardException {
       switch (t.getJDBCTypeId()) {
       case Types.TINYINT:
           return val != null ? TypeId.SMALLINT_MAXWIDTH : 0; // FIXME
       case Types.INTEGER:
           return val != null ? TypeId.INT_MAXWIDTH : 0;
       case Types.SMALLINT:
           return val != null ? TypeId.SMALLINT_MAXWIDTH : 0;
       case Types.BIGINT:
           return val != null ? TypeId.LONGINT_MAXWIDTH: 0;
       case Types.DECIMAL:
            if (val != null) {
               NumberDataValue constantDecimal =
                        getDataValueFactory().getDecimalDataValue((String) val);
               int precision = constantDecimal.getDecimalValuePrecision();
               int scal = constantDecimal.getDecimalValueScale();
               /* be consistent with our convention on maxwidth, see also
                * exactNumericType(), otherwise we get format problem, b 3923
                */
               return DataTypeUtilities.computeMaxWidth(precision, scal);
            } else {
                return TypeId.DECIMAL_MAXWIDTH;
            }
       case Types.DOUBLE:
           return val != null ? TypeId.DOUBLE_MAXWIDTH : 0;
       case Types.REAL:
           return val != null ? TypeId.REAL_MAXWIDTH : 0;
       default:
           if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
           }
           return 0;
       }
    }

    private int getNodeType(TypeId t) {
       switch (t.getJDBCTypeId()) {
       case Types.TINYINT:
           return C_NodeTypes.TINYINT_CONSTANT_NODE;
       case Types.INTEGER:
           return C_NodeTypes.INT_CONSTANT_NODE;
       case Types.SMALLINT:
           return C_NodeTypes.SMALLINT_CONSTANT_NODE;
       case Types.BIGINT:
           return C_NodeTypes.BIGINT_CONSTANT_NODE;
       case Types.DECIMAL:
            return C_NodeTypes.DECIMAL_CONSTANT_NODE;
       case Types.DOUBLE:
           return C_NodeTypes.DOUBLE_CONSTANT_NODE;
       case Types.REAL:
           return C_NodeTypes.REAL_CONSTANT_NODE;
       default:
           if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
           }
           return 0;
       }
    }

    private void setValue(TypeId t, Object value ) throws StandardException {
       switch (t.getJDBCTypeId()) {
       case Types.TINYINT:
           setValue(new SQLTinyint((Byte)value));
            break;
       case Types.INTEGER:
           setValue(new SQLInteger((Integer)value));
            break;
       case Types.SMALLINT:
           setValue(new SQLSmallint((Short)value));
            break;
       case Types.BIGINT:
           setValue(new SQLLongint((Long)value));
            break;
       case Types.DECIMAL:
            NumberDataValue constantDecimal =
                    getDataValueFactory().getDecimalDataValue((String)value);
            setValue(constantDecimal);
            break;
       case Types.DOUBLE:
           setValue(new SQLDouble((Double)value));
            break;
       case Types.REAL:
           setValue(new SQLReal((Float)value));
            break;
       default:
           if (SanityManager.DEBUG) {
                SanityManager.NOTREACHED();
           }
       }
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
    @Override
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
			mb.push(value.getString());
            mb.pushNewComplete(1);
			break;
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			mb.push(value.getDouble());
			break;
        case C_NodeTypes.REAL_CONSTANT_NODE:
			mb.push(value.getFloat());
			break;
        case C_NodeTypes.BIGINT_CONSTANT_NODE:
			mb.push(value.getLong());
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
