/*

   Derby - Class org.apache.derby.impl.sql.compile.UserTypeConstantNode

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

import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.TypeId;

/**
	User type constants.  These are created by built-in types
	that use user types as their implementation. This could also
	potentially be used by an optimizer that wanted to store plans
	for frequently-used parameter values.

	This is also used to represent nulls in user types, which occurs
	when NULL is inserted into or supplied as the update value for
   a user type column.
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973

 */
class UserTypeConstantNode extends ConstantNode {
	/*
	** This value field hides the value in the super-type.  It is here
	** Because user-type constants work differently from built-in constants.
	** User-type constant values are stored as Objects, while built-in
	** constants are stored as StorableDataValues.
	**
	** RESOLVE: This is a bit of a mess, and should be fixed.  All constants
	** should be represented the same way.
	*/
    Object  val;
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973

    UserTypeConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(t, true, TypeDescriptor.MAXIMUM_WIDTH_UNKNOWN, cm);
    }

    UserTypeConstantNode(Date d, ContextManager cm)
            throws StandardException {
        super(TypeId.getBuiltInTypeId(Types.DATE),
              d == null,
              TypeId.DATE_MAXWIDTH,
              cm);
        setValue(new SQLDate(d));
        val = d;
    }

    UserTypeConstantNode(Time t, ContextManager cm)
            throws StandardException {
        super(TypeId.getBuiltInTypeId(Types.TIME),
              t == null,
              TypeId.TIME_MAXWIDTH,
              cm);
        setValue(new SQLTime(t));
        val = t;
    }

    UserTypeConstantNode(Timestamp t, ContextManager cm)
            throws StandardException {
        super(TypeId.getBuiltInTypeId(Types.TIMESTAMP),
                t == null,
                TypeId.TIMESTAMP_MAXWIDTH,
                cm);
        setValue(new SQLTimestamp(t));
        val = t;
    }

    /**
     * @param dvd Must contain a Date, Time or Timestamp value
     * @param cm context manager
     * @throws StandardException
     */
    UserTypeConstantNode(DataValueDescriptor dvd, ContextManager cm)
            throws StandardException {
        super(getTypeId(dvd),
              dvd == null,
              getWidth(dvd),
              cm);
        setValue(dvd);
        val = dvd;
    }

    private static TypeId getTypeId(DataValueDescriptor dvd) {
        if (dvd != null) {
            switch (dvd.getTypeFormatId()) {
                case StoredFormatIds.SQL_DATE_ID:
                    return TypeId.getBuiltInTypeId(Types.DATE);
                case StoredFormatIds.SQL_TIME_ID:
                    return TypeId.getBuiltInTypeId(Types.TIME);
                case StoredFormatIds.SQL_TIMESTAMP_ID:
                    return TypeId.getBuiltInTypeId(Types.TIMESTAMP);
                default:
                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT(
                                "Unexpected class " + dvd.getClass().getName());
                    }
                    return null;
            }
        } else {
            return null;
        }
    }

    private static int getWidth(DataValueDescriptor dvd) {
        if (dvd != null) {
            switch (dvd.getTypeFormatId()) {
                case StoredFormatIds.SQL_DATE_ID:
                    return TypeId.DATE_MAXWIDTH;
                case StoredFormatIds.SQL_TIME_ID:
                    return TypeId.TIME_MAXWIDTH;
                case StoredFormatIds.SQL_TIMESTAMP_ID:
                    return TypeId.TIMESTAMP_MAXWIDTH;
                default:
                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT(
                                "Unexpected class " + dvd.getClass().getName());
                    }
                    return 0;
            }
        } else {
            return 0;
        }
    }

	/**
	 * Return the object value of this user defined type.
	 *
	 * @return	the value of this constant. can't use getValue() for this.
	 *			getValue() returns the DataValueDescriptor for the built-in
	 *			types that are implemented as user types (date, time, timestamp)
	 */
    public  Object  getObjectValue() { return val; }

	/**
	 * Return whether or not this node represents a typed null constant.
	 *
	 */
    @Override
    boolean isNull()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        return (val == null);
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
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    Object getConstantValueAsObject()
	{
        return val;
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
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException {

		TypeCompiler		tc = getTypeCompiler();
        String fieldType = tc.interfaceName();

		/*
		** NOTE: DO NOT CALL THE CONSTRUCTOR TO GENERATE ANYTHING.  IT HAS
		** A DIFFERENT value FIELD.
		*/
		
		/* Are we generating a SQL null value? */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        if (val == null)
	    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2583
			acb.generateNull(mb, tc, getTypeServices().getCollationType());
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

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            mb.push(val.toString());
			mb.callMethod(VMOpcode.INVOKESTATIC, typeName, "valueOf", typeName, 1);

			LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

//IC see: https://issues.apache.org/jira/browse/DERBY-2583
			acb.generateDataValue(mb, tc, getTypeServices().getCollationType(), field);
		}
	}


	/**
	 * Should never be called for UserTypeConstantNode because
	 * we have our own generateExpression().
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
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
