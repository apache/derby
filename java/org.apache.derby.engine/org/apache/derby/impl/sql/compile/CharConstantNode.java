/*

   Derby - Class org.apache.derby.impl.sql.compile.CharConstantNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;

public final class CharConstantNode extends ConstantNode
{

    // Allowed kinds
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    final static int K_CHAR = 0;
    final static int K_VARCHAR = 1;
    final static int K_LONGVARCHAR = 2;
    final static int K_CLOB = 3;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    CharConstantNode(String value, ContextManager cm)
            throws StandardException {
        super(TypeId.CHAR_ID,
              value == null, // nullable?
              (value != null) ? value.length() : 0,
              cm);

        setValue(getDataValueFactory().getCharDataValue(value));
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        kind = K_CHAR;
    }

    CharConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(t, true, 0, cm);
        kind = K_CHAR;
    }

    /**
     * @param kind The node kind
     * @param t    The type id
     * @param cm   The context manager
     * @throws StandardException
     */
    CharConstantNode(int kind, TypeId t, ContextManager cm)
            throws StandardException {
        super(t, true, 0, cm);
        this.kind = kind;
    }

    /**
     * Constructor for a CharConstantNode of a specific length.
     *
     * @param newValue A String containing the value of the constant
     * @param newLength The length of the new value of the constant
     * @param cm
     * @throws StandardException
     */
    CharConstantNode(String newValue, int newLength, ContextManager cm)
            throws StandardException {

        super(TypeId.CHAR_ID,
              newValue == null,
              newLength,
              cm);

        kind = K_CHAR;
//IC see: https://issues.apache.org/jira/browse/DERBY-673

        if (newValue.length() > newLength) {
           throw StandardException.newException(
                    SQLState.LANG_STRING_TRUNCATION,
                    "CHAR",
                    newValue,
                    String.valueOf(newLength));
		}

        // Blank pad the string if necessesary
       while (newValue.length() < newLength) {
           newValue = newValue + ' ';
		}

       setValue(getDataValueFactory().getCharDataValue(newValue));
	}

	/**
	 * Return the value from this CharConstantNode
	 *
	 * @return	The value of this CharConstantNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String  getString() throws StandardException
	{
		return value.getString();
	}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */

	//public int	getLength() throws StandardException
	//{
	//	return value.getLength();
	//}

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
	Object getConstantValueAsObject() throws StandardException 
	{
		return value.getString();
	}
	
    @Override
    ValueNode bindExpression(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        FromList fromList,
        SubqueryList subqueryList,
        List<AggregateNode> aggregates) throws StandardException
	{
		//The DTD for this character constant should get its collation type
		//from the schema it is getting compiled in.
		setCollationUsingCompilationSchema();
	    //Once we have the collation type, we should check if the value
	    //associated with this node should change from 
	    //SQLChar/SQLVarchar/SQLLongvarchar/SQLClob
	    //to
	    //CollatorSQLChar/CollatoSQLVarchar/CollatoSQLLongvarchar/CollatoSQLClob.
	    //By default, the value associated with char constants are SQLxxx
	    //kind because that is what is needed for UCS_BASIC collation. But
	    //if at this bind time, we find that the char constant's collation
	    //type is territory based, then we should change value from SQLxxx
	    //to CollatorSQLxxx. That is what is getting done below.
//IC see: https://issues.apache.org/jira/browse/DERBY-2335
//IC see: https://issues.apache.org/jira/browse/DERBY-2335
	    value = ((StringDataValue)value).getValue(
	    		getLanguageConnectionContext().getDataValueFactory().getCharacterCollator(
	    				getTypeServices().getCollationType()));
		return this;
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException
	{
		// The generated java is the expression:
		// "#getString()"
		mb.push(getString());
	}

    @Override
    boolean isSameNodeKind(ValueNode o) {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return super.isSameNodeKind(o) && ((CharConstantNode)o).kind == kind;
    }}
