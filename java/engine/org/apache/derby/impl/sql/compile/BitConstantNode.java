/*

   Derby - Class org.apache.derby.impl.sql.compile.BitConstantNode

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
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.util.StringUtil;


class BitConstantNode extends ConstantNode
{
    /**
     * @param t The TypeId for the type of the node
     * @param cm context manager
     * @throws StandardException
     */
    BitConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(t, true, 0, cm);
    }


    /**
     * @param hexString hexadecimally coded bit string
     * @param bitLength desired length of the bit string
     * @param cm context manager
     * @throws StandardException
     */
    BitConstantNode(String hexString, int bitLength, ContextManager cm)
            throws StandardException {
        super(TypeId.getBuiltInTypeId(Types.BINARY), false, bitLength, cm);
        byte[] nv = StringUtil.fromHexString(hexString, 0, hexString.length());
        BitDataValue dvd = getDataValueFactory().getBitDataValue(nv);
		dvd.setWidth(bitLength, 0, false);

        setValue(dvd);
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
		return value.getBytes();
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
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
}
