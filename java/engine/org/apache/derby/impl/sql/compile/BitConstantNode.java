/*

   Derby - Class org.apache.derby.impl.sql.compile.BitConstantNode

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.sql.Types;

public class BitConstantNode extends ConstantNode
{

	private int bitLength;


	/**
	 * Initializer for a BitConstantNode.
	 *
	 * @param arg1	A Bit containing the value of the constant
	 *
	 * - OR -
	 *
	 * @param arg1	The TypeId for the type of the node
	 *
	 * @exception StandardException
	 */

	public void init(
					Object arg1)
		throws StandardException
	{
		super.init(
					arg1,
					Boolean.TRUE,
					ReuseFactory.getInteger(0));
	}

	public void init(
					Object arg1, Object arg2)
		throws StandardException
	{
		String a1 = (String) arg1;

		byte[] nv = org.apache.derby.iapi.util.StringUtil.fromHexString(a1, 0, a1.length()); 

		Integer bitLengthO = (Integer) arg2;
		bitLength = bitLengthO.intValue();

		init(
			TypeId.getBuiltInTypeId(Types.BINARY),
			Boolean.FALSE,
			bitLengthO);

		org.apache.derby.iapi.types.BitDataValue dvd = getDataValueFactory().getBitDataValue(nv);

		dvd.setWidth(bitLength, 0, false);

		setValue(dvd);
	}

	/**
	 * Initializer for non-numeric types.  Needed for our subclasses
	 *
	 * @param typeCompilationFactory	The factory to get the
	 *									DataTypeServicesFactory from
	 * @param typeId	The Type ID of the datatype
	 * @param nullable	True means the constant is nullable
	 * @param maximumWidth	The maximum number of bytes in the data value
	 *
	 * @exception StandardException
	 */
	public void init(
			Object typeId,
			Object nullable,
			Object maximumWidth)
		throws StandardException
	{
		init(
					typeId,
					ReuseFactory.getInteger(0),
					ReuseFactory.getInteger(0),
					nullable,
					maximumWidth);
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
		return value.getBytes();
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @return		The compiled Expression, 
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		byte[] bytes = value.getBytes();

		String hexLiteral = org.apache.derby.iapi.util.StringUtil.toHexString(bytes, 0, bytes.length);

		mb.push(hexLiteral);
		mb.push(0);
		mb.push(hexLiteral.length());

		mb.callMethod(VMOpcode.INVOKESTATIC, "org.apache.derby.iapi.util.StringUtil", "fromHexString",
						"byte[]", 3);
	}

	
	void setConstantWidth(ExpressionClassBuilder acb, MethodBuilder mb) {
		if ((bitLength % 8) != 0) {
			// temp for binary types.
			mb.cast("org.apache.derby.iapi.types.VariableSizeDataValue");
			mb.push(bitLength);
			mb.push(0);
			mb.push(false);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "setWidth", "org.apache.derby.iapi.types.DataValueDescriptor", 3);
		}
	}
}
