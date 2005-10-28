/*

   Derby - Class org.apache.derby.impl.sql.compile.UserTypeConstantNode

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.UserDataValue;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.types.*;

import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
	User type constants.  These are created by built-in types
	that use user types as their implementation. This could also
	potentially be used by an optimizer that wanted to store plans
	for frequently-used parameter values.

	This is also used to represent nulls in user types, which occurs
	when NULL is inserted into or supplied as the update value for
	a usertype column.

	@author ames
 */
public class UserTypeConstantNode extends ConstantNode {
	/*
	** This value field hides the value in the super-type.  It is here
	** Because user-type constants work differently from built-in constants.
	** User-type constant values are stored as Objects, while built-in
	** constants are stored as StorableDataValues.
	**
	** RESOLVE: This is a bit of a mess, and should be fixed.  All constants
	** should be represented the same way.
	*/
	Object	value;

	/**
	 * Initializer for a typed null node
	 * or a date, time, or timestamp value
	 *
	 * @param arg1	The TypeId for the type of the node
	 * @param arg2	The factory to get the TypeId
	 *			and DataTypeServices factories from.
	 *
	 * - OR -
	 *
	 * @param arg1 the date, time, or timestamp value
	 *
	 * @exception StandardException thrown on failure
	 */
	public void init(Object arg1)
			throws StandardException {
        DataValueDescriptor dvd = null;
        
		if (arg1 instanceof TypeId)
		{
			super.init(
					arg1,
					Boolean.TRUE,
					ReuseFactory.getInteger(
										TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN));
		}
		else
		{
			Integer maxWidth = null;
			TypeId	typeId = null;

            if( arg1 instanceof DataValueDescriptor)
                dvd = (DataValueDescriptor) arg1;
			if (arg1 instanceof Date
                || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_DATE_ID))
			{
				maxWidth = ReuseFactory.getInteger(TypeId.DATE_MAXWIDTH);
				typeId = TypeId.getBuiltInTypeId(Types.DATE);
			}
			else if (arg1 instanceof Time
                     || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_TIME_ID))
			{
				maxWidth = ReuseFactory.getInteger(TypeId.TIME_MAXWIDTH);
				typeId = TypeId.getBuiltInTypeId(Types.TIME);
			}
			else if (arg1 instanceof Timestamp
                     || (dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_TIMESTAMP_ID))
			{
				maxWidth = ReuseFactory.getInteger(TypeId.TIMESTAMP_MAXWIDTH);
				typeId = TypeId.getBuiltInTypeId(Types.TIMESTAMP);
			}
			else
			{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
							"Unexpected class " + arg1.getClass().getName());
				}
			}

			super.init( 
				typeId,
				(arg1 == null) ? Boolean.TRUE : Boolean.FALSE,
				maxWidth);

            if( dvd != null)
                setValue( dvd);
			else if (arg1 instanceof Date)
			{
				setValue(getDataValueFactory().getDataValue((Date) arg1));
			}
			else if (arg1 instanceof Time)
			{
				setValue(getDataValueFactory().getDataValue((Time) arg1));
			}
			else if (arg1 instanceof Timestamp)
			{
				setValue(getDataValueFactory().getDataValue((Timestamp) arg1));
			}

			value = arg1;
		}
	}

	/**
	 * Return the object value of this user defined type.
	 *
	 * @return	the value of this constant. can't use getValue() for this.
	 *			getValue() returns the DataValueDescriptor for the built-in
	 *			types that are implemented as user types (date, time, timestamp)
	 */
    public	Object	getObjectValue() { return value; }

	/**
	 * Return whether or not this node represents a typed null constant.
	 *
	 */
	public boolean isNull()
	{
		return (value == null);
	}

	/**
	 * Return the value of this user defined type as a Storable
	 *
	 * @return	the value of this constant as a UserType
	 * @exception StandardException thrown on failure
	 */
    public	DataValueDescriptor	getStorableValue()
			throws StandardException
	{
        if( value instanceof DataValueDescriptor)
            return ((DataValueDescriptor) value).getClone();
        
		DataValueFactory			dvf = getDataValueFactory();
		TypeId			typeID = getTypeId();
		String						typeName = typeID.getSQLTypeName();

		if ( typeName.equals( TypeId.DATE_NAME ) )
		{
			return	new SQLDate((Date) value);
		}
		else if ( typeName.equals( TypeId.TIME_NAME ) )
		{
			return	new SQLTime( (Time) value);
		}
		else if ( typeName.equals( TypeId.TIMESTAMP_NAME ) )
		{
			return	new SQLTimestamp( (Timestamp) value);
		}
		else
		{
			return	dvf.getDataValue( value, (UserDataValue) null );
		}
	}

	/**
	 * Sets the object value of this user defined type
	 *
	 * @param	the value of this constant. can't use setValue() for this.
	 */
    public	void	setObjectValue( Object newValue ) { value = newValue; }

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */
	//public int	getLength() throws StandardException {
	//	return TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN;
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
	 */
	public Object getConstantValueAsObject()
	{
		return value;
	}

	/**
	 * For a UserTypeConstantNode, we have to store away the object somewhere
	 * and have a way to get it back at runtime.
	 * These objects are serializable.  This gives us at least two options:
	 * 1) serialize it out into a byte array field, and serialize
	 *	  it back in when needed, from the field.
	 * 2) have an array of objects in the prepared statement and a #,
	 *	  to find the object directly. Because it is serializable, it
	 *	  will store with the rest of the executable just fine.
	 * Choice 2 gives better performance -- the ser/deser cost is paid
	 * on database access for the statement, not for each execution of it.
	 * However, it requires some infrastructure support from prepared
	 * statements.  For now, we take choice 3, and make some assumptions
	 * about available methods on the user type.  This choice has the
	 * shortcoming that it will not work for arbitrary user types.
	 * REVISIT and implement choice 2 when a general solution is needed.
	 * <p>
	 * A null is generated as a Null value cast to the type of
	 * the constant node.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException {

		TypeCompiler		tc = getTypeCompiler();
        String fieldType = tc.interfaceName();

		/*
		** NOTE: DO NOT CALL THE CONSTRUCTOR TO GENERATE ANYTHING.  IT HAS
		** A DIFFERENT value FIELD.
		*/
		
		/* Are we generating a SQL null value? */
	    if (value == null)
	    {
			acb.generateNull(mb, tc);
	    }
        // The code generated here is invoked when the generated class is constructed. However the prepared statement
        // is not set into the activation class when it is constructed, but later. So we cannot use the getSavedObject
        // method to retrieve the value.
//         else if( value instanceof DataValueDescriptor)
//         {
//             acb.pushThisAsActivation( mb);
//             mb.callMethod( VMOpcode.INVOKEINTERFACE,
//                            null,
//                            "getPreparedStatement",
//                            ClassName.ExecPreparedStatement,
//                            0);
//             mb.push( acb.addItem( value));
//             mb.callMethod( VMOpcode.INVOKEINTERFACE,
//                            null,
//                            "getSavedObject",
//                            "java.lang.Object",
//                            1);
//             mb.cast( fieldType);
//         }
		else
		{
			/*
				The generated java is the expression:
					<java type name>.valueOf("<value.toString>")

				super.generateValue will wrap this expression in
				the appropriate column constructor.

				If the type doesn't have a valueOf method, then we will
				give an error.  We have to assume that valueOf will
				reconstruct the object from a String literal.  If this is
				a false assumption, some other object may be constructed,
				or a runtime error may result due to valueOf failing.
		 	*/
			String typeName = getTypeId().getCorrespondingJavaTypeName();

			mb.push(value.toString());
			mb.callMethod(VMOpcode.INVOKESTATIC, typeName, "valueOf", typeName, 1);

			LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

			acb.generateDataValue(mb, tc, field);
		}
	}


	/**
	 * Should never be called for UserTypeConstantNode because
	 * we have our own generateExpression().
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
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("geneateConstant() not expected to be called for UserTypeConstantNode because we have implemented our own generateExpression().");
		}
	}
}
