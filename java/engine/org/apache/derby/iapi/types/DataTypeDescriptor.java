/*

   Derby - Class org.apache.derby.iapi.types.DataTypeDescriptor

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.TypeDescriptorImpl;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;


import org.apache.derby.iapi.reference.SQLState;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.Types;

/**
 * This is an implementation of DataTypeDescriptor from the generic language
 * datatype module interface.
 *
 * @author Jeff Lichtman
 * @version 1.0
 */

public final class DataTypeDescriptor implements TypeDescriptor, Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	/*
	** Static creators
	*/
	/**
	 * Get a descriptor that corresponds to a builtin JDBC type.
	 *
	 * @param jdbcType	The int type of the JDBC type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type
	 */
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		int	jdbcType
	)
	{
		return DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType, true);
	}
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		int	jdbcType,
		int length
	)
	{
		return DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType, true, length);
	}

	/**
	 * Get a descriptor that corresponds to a builtin JDBC type.
	 *
	 * @param jdbcType	The int type of the JDBC type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type
	 */
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		int	jdbcType, 
		boolean	isNullable
	)
	{
		TypeId typeId = TypeId.getBuiltInTypeId(jdbcType);
		if (typeId == null)
		{
			return null;
		}

		return new DataTypeDescriptor(typeId, isNullable);
	}
	/**
	 * Get a descriptor that corresponds to a builtin JDBC type.
	 *
	 * @param jdbcType	The int type of the JDBC type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type
	 */
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		int	jdbcType, 
		boolean	isNullable,
		int maxLength
	)
	{
		TypeId typeId = TypeId.getBuiltInTypeId(jdbcType);
		if (typeId == null)
		{
			return null;
		}

		return new DataTypeDescriptor(typeId, isNullable, maxLength);
	}
	/**
	 * Get a DataTypeServices that corresponds to a builtin SQL type
	 *
	 * @param javaTypeName	The name of the Java type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type (only for 'char')
	 */
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		String	sqlTypeName
	)
	{
		return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName), true);
	}
	/**
	 * Get a DataTypeServices that corresponds to a builtin SQL type
	 *
	 * @param javaTypeName	The name of the Java type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type (only for 'char')
	 */
	public static DataTypeDescriptor getBuiltInDataTypeDescriptor
	(
		String	sqlTypeName,
		int length
	)
	{
		return new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName), true, length);
	}
	/**
	 * Get a DataTypeServices that corresponds to a Java type
	 *
	 * @param javaTypeName	The name of the Java type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type (only for 'char')
	 */
	public static DataTypeDescriptor getSQLDataTypeDescriptor
	(
		String	javaTypeName
	)
	{
			return DataTypeDescriptor.getSQLDataTypeDescriptor(javaTypeName, true);
	}

	/**
	 * Get a DataTypeServices that corresponds to a Java type
	 *
	 * @param javaTypeName	The name of the Java type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type (only for 'char')
	 */
	public static DataTypeDescriptor getSQLDataTypeDescriptor
	(
		String	javaTypeName, 
		boolean	isNullable
	)
	{
		TypeId typeId = TypeId.getSQLTypeForJavaType(javaTypeName);
		if (typeId == null)
		{
			return null;
		}

		return new DataTypeDescriptor(typeId, isNullable);
	}

	/**
	 * Get a DataTypeDescriptor that corresponds to a Java type
	 *
	 * @param javaTypeName	The name of the Java type for which to get
	 *						a corresponding SQL DataTypeDescriptor
	 * @param precision	The number of decimal digits
	 * @param scale		The number of digits after the decimal point
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 * @param maximumWidth	The maximum width of a data value
	 *			represented by this type.
	 *
	 * @return	A new DataTypeDescriptor that corresponds to the Java type.
	 *			A null return value means there is no corresponding SQL type.
	 */
	public static DataTypeDescriptor getSQLDataTypeDescriptor
	(
		String	javaTypeName, 
		int 	precision,
		int 	scale, 
		boolean	isNullable, 
		int 	maximumWidth
	)
	{
		TypeId typeId = TypeId.getSQLTypeForJavaType(javaTypeName);
		if (typeId == null)
		{
			return null;
		}

		return new DataTypeDescriptor(typeId,
											precision,
											scale,
											isNullable,
											maximumWidth);
	}
	/*
	** Instance fields & methods
	*/

	private TypeDescriptorImpl	typeDescriptor;
	private TypeId			typeId;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	DataTypeDescriptor() {}

	/**
	 * Constructor for use with numeric types
	 *
	 * @param typeId	The typeId of the type being described
	 * @param precision	The number of decimal digits.
	 * @param scale		The number of digits after the decimal point.
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 * @param maximumWidth	The maximum number of bytes for this datatype
	 */
	public DataTypeDescriptor(TypeId typeId, int precision, int scale,
		boolean isNullable, int maximumWidth)
	{
		this.typeId = typeId;
		typeDescriptor = new TypeDescriptorImpl(typeId.getBaseTypeId(),
												precision,
												scale,
												isNullable,
												maximumWidth);
	}

	/**
	 * Constructor for use with non-numeric types
	 *
	 * @param typeId	The typeId of the type being described
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 * @param maximumWidth	The maximum number of bytes for this datatype
	 */
	public DataTypeDescriptor(TypeId typeId, boolean isNullable,
		int maximumWidth)
	{
		this.typeId = typeId;
		typeDescriptor = new TypeDescriptorImpl(typeId.getBaseTypeId(),
												isNullable,
												maximumWidth);
	}


	public DataTypeDescriptor(TypeId typeId, boolean isNullable) {

		this.typeId = typeId;
		typeDescriptor = new TypeDescriptorImpl(typeId.getBaseTypeId(),
												typeId.getMaximumPrecision(),
												typeId.getMaximumScale(),
												isNullable,
												typeId.getMaximumMaximumWidth());
	}
	public DataTypeDescriptor(DataTypeDescriptor source, boolean isNullable)
	{
		this.typeId = source.typeId;
		typeDescriptor = new TypeDescriptorImpl(source.typeDescriptor,
												source.getPrecision(),
												source.getScale(),
												isNullable,
												source.getMaximumWidth());
	}

	/**
	 * Constructor for internal uses only.  
	 * (This is useful when the precision and scale are potentially wider than
	 * those in the source, like when determining the dominant data type.)
	 *
	 * @param source	The DTSI to copy
	 * @param precision	The number of decimal digits.
	 * @param scale		The number of digits after the decimal point.
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 * @param maximumWidth	The maximum number of bytes for this datatype
	 */
	public DataTypeDescriptor(DataTypeDescriptor source, 
								int precision,
								int scale,
								boolean isNullable,
								int maximumWidth)
	{
		this.typeId = source.typeId;
		typeDescriptor = new TypeDescriptorImpl(source.typeDescriptor,
												precision,
												scale,
												isNullable,
												maximumWidth);
	}

	/**
	 * Constructor for internal uses only
	 *
	 * @param source	The DTSI to copy
	 * @param isNullable	TRUE means it could contain NULL, FALSE means
	 *			it definitely cannot contain NULL.
	 * @param maximumWidth	The maximum number of bytes for this datatype
	 */
	public DataTypeDescriptor(DataTypeDescriptor source, boolean isNullable,
		int maximumWidth)
	{
		this.typeId = source.typeId;
		typeDescriptor = new TypeDescriptorImpl(source.typeDescriptor,
												isNullable,
												maximumWidth);
	}

	/**
	 * Constructor for use in reconstructing a DataTypeDescriptor from a
	 * TypeDescriptorImpl and a TypeId
	 *
	 * @param source	The TypeDescriptorImpl to construct this DTSI from
	 */
	public DataTypeDescriptor(TypeDescriptorImpl source, TypeId typeId)
	{
		typeDescriptor = source;
		this.typeId = typeId;;
	}

	/* DataTypeDescriptor Interface */
	public DataValueDescriptor normalize(DataValueDescriptor source,
										DataValueDescriptor cachedDest)
			throws StandardException
	{
		if (SanityManager.DEBUG) {
			if (cachedDest != null) {
				if (!getTypeId().isUserDefinedTypeId()) {
					String t1 = getTypeName();
					String t2 = cachedDest.getTypeName();
					if (!t1.equals(t2)) {

						if (!(((t1.equals("DECIMAL") || t1.equals("NUMERIC"))
							&& (t2.equals("DECIMAL") || t2.equals("NUMERIC"))) ||
							(t1.startsWith("INT") && t2.startsWith("INT"))))  //INT/INTEGER

							SanityManager.THROWASSERT(
								"Normalization of " + t2 + " being asked to convert to " + t1);
					}
				}
			}
		}

		if (source.isNull())
		{
			if (!isNullable())
				throw StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL,"");

			if (cachedDest == null)
				cachedDest = getNull();
			else
				cachedDest.setToNull();
		} else {

			if (cachedDest == null)
				cachedDest = getNull();

			int jdbcId = getJDBCTypeId();

			cachedDest.normalize(this, source);
			//doing the following check after normalize so that normalize method would get called on long varchs and long varbinary
			//Need normalize to be called on long varchar for bug 5592 where we need to enforce a lenght limit in db2 mode
			if ((jdbcId == Types.LONGVARCHAR) || (jdbcId == Types.LONGVARBINARY)) {
				// special case for possible streams
				if (source.getClass() == cachedDest.getClass()) 
					return source;
			}

		}
		return cachedDest;
	}
	
	/**
	 * Get the dominant type (DataTypeDescriptor) of the 2.
	 * For variable length types, the resulting type will have the
	 * biggest max length of the 2.
	 * If either side is nullable, then the result will also be nullable.
	 *
	 * @param otherDTS	DataTypeDescriptor to compare with.
	 * @param cf		A ClassFactory
	 *
	 * @return DataTypeDescriptor  DTS for dominant type
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataTypeDescriptor getDominantType(DataTypeDescriptor otherDTS, ClassFactory cf)
			throws StandardException
	{
		boolean				nullable;
		TypeId				thisType;
		TypeId				otherType;
		DataTypeDescriptor	higherType = null;
		DataTypeDescriptor	lowerType = null;
		int					maximumWidth;
		int					precision = getPrecision();
		int					scale = getScale();

		thisType = getTypeId();
		otherType = otherDTS.getTypeId();

		/* The result is nullable if either side is nullable */
		nullable = isNullable() || otherDTS.isNullable();

		/*
		** The result will have the maximum width of both sides
		*/
		maximumWidth = (getMaximumWidth() > otherDTS.getMaximumWidth())
			? getMaximumWidth() : otherDTS.getMaximumWidth();

		/* We need 2 separate methods of determining type dominance - 1 if both
		 * types are system built-in types and the other if at least 1 is
		 * a user type. (typePrecedence is meaningless for user types.)
		 */
		if (thisType.systemBuiltIn() && otherType.systemBuiltIn())
		{
			TypeId  higherTypeId;
			TypeId  lowerTypeId;
			if (thisType.typePrecedence() > otherType.typePrecedence())
			{
				higherType = this;
				lowerType = otherDTS;
				higherTypeId = thisType;
				lowerTypeId = otherType;
			}
			else
			{
				higherType = otherDTS;
				lowerType = this;
				higherTypeId = otherType;
				lowerTypeId = thisType;
			}

			//Following is checking if higher type argument is real and other argument is decimal/bigint/integer/smallint,
			//then result type should be double
			if (higherTypeId.isRealTypeId() && (!lowerTypeId.isRealTypeId()) && lowerTypeId.isNumericTypeId())
			{
				higherType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);
				higherTypeId = TypeId.getBuiltInTypeId(Types.DOUBLE);
			}
			/*
			** If we have a DECIMAL/NUMERIC we have to do some
			** extra work to make sure the resultant type can
			** handle the maximum values for the two input
			** types.  We cannot just take the maximum for
			** precision.  E.g. we want something like:
			**
			**		DEC(10,10) and DEC(3,0) => DEC(13,10)
			**
			** (var)char type needs some conversion handled later.
			*/
			if (higherTypeId.isDecimalTypeId() && (!lowerTypeId.isStringTypeId()))
			{
				precision = higherTypeId.getPrecision(this, otherDTS);
				if (precision > 31) precision = 31; //db2 silently does this and so do we
				scale = higherTypeId.getScale(this, otherDTS);

				/* maximumWidth needs to count possible leading '-' and
				 * decimal point and leading '0' if scale > 0.  See also
				 * sqlgrammar.jj(exactNumericType).  Beetle 3875
				 */
				maximumWidth = (scale > 0) ? precision + 3 : precision + 1;
			}
			else if (thisType.typePrecedence() != otherType.typePrecedence())
			{
				precision = higherType.getPrecision();
				scale = higherType.getScale();

				/* GROSS HACKS:
				 * If we are doing an implicit (var)char->(var)bit conversion
				 * then the maximum width for the (var)char as a (var)bit
				 * is really 16 * its width as a (var)char.  Adjust
				 * maximumWidth accordingly.
				 * If we are doing an implicit (var)char->decimal conversion
				 * then we need to increment the decimal's precision by
				 * 2 * the maximum width for the (var)char and the scale
				 * by the maximum width for the (var)char. The maximumWidth
				 * becomes the new precision + 3.  This is because
				 * the (var)char could contain any decimal value from XXXXXX
				 * to 0.XXXXX.  (In other words, we don't know which side of the
				 * decimal point the characters will be on.)
				 */
				if (lowerTypeId.isStringTypeId())
				{
					if (higherTypeId.isBitTypeId() &&
						! (higherTypeId.isLongConcatableTypeId()))
					{
						if (lowerTypeId.isLongConcatableTypeId())
						{
							if (maximumWidth > (Integer.MAX_VALUE / 16))
								maximumWidth = Integer.MAX_VALUE;
							else
								maximumWidth *= 16;
						}
						else
						{
							int charMaxWidth;

							int fromWidth = lowerType.getMaximumWidth();
							if (fromWidth > (Integer.MAX_VALUE / 16))
								charMaxWidth = Integer.MAX_VALUE;
							else
								charMaxWidth = 16 * fromWidth;

							maximumWidth = (maximumWidth >= charMaxWidth) ?
												maximumWidth : charMaxWidth;
						}
					}
				}

				/*
				 * If we are doing an implicit (var)char->decimal conversion
				 * then the resulting decimal's precision could be as high as 
				 * 2 * the maximum width (precisely 2mw-1) for the (var)char
				 * and the scale could be as high as the maximum width
				 * (precisely mw-1) for the (var)char.
				 * The maximumWidth becomes the new precision + 3.  This is
				 * because the (var)char could contain any decimal value from
				 * XXXXXX to 0.XXXXX.  (In other words, we don't know which
				 * side of the decimal point the characters will be on.)
				 *
				 * We don't follow this algorithm for long varchar because the
				 * maximum length of a long varchar is maxint, and we don't
				 * want to allocate a huge decimal value.  So in this case,
				 * the precision, scale, and maximum width all come from
				 * the decimal type.
				 */
				if (lowerTypeId.isStringTypeId() &&
					! (lowerTypeId.isLongConcatableTypeId()) &&
					higherTypeId.isDecimalTypeId() )
				{
					int charMaxWidth = lowerType.getMaximumWidth();
					int charPrecision;

					/*
					** Be careful not to overflow when calculating the
					** precision.  Remember that we will be adding
					** three to the precision to get the maximum width.
					*/
					if (charMaxWidth > (Integer.MAX_VALUE - 3) / 2)
						charPrecision = Integer.MAX_VALUE - 3;
					else
						charPrecision = charMaxWidth * 2;

					if (precision < charPrecision)
						precision = charPrecision;

					if (scale < charMaxWidth)
						scale = charMaxWidth;

					maximumWidth = precision + 3;
				}
			}
		}
		else
		{
			/* At least 1 type is not a system built-in type */
			ClassInspector		cu = cf.getClassInspector();

			TypeId thisCompType = (TypeId) thisType;
			TypeId otherCompType = (TypeId) otherType;

			if (cu.assignableTo(thisCompType.getCorrespondingJavaTypeName(),
							    otherCompType.getCorrespondingJavaTypeName()))
			{
				higherType = otherDTS;
			}
			else
			{
				if (SanityManager.DEBUG)
						SanityManager.ASSERT(
							cu.assignableTo(otherCompType.getCorrespondingJavaTypeName(),
									thisCompType.getCorrespondingJavaTypeName()),
							otherCompType.getCorrespondingJavaTypeName() +
							" expected to be assignable to " +
							thisCompType.getCorrespondingJavaTypeName());

				higherType = this;
			}
			precision = higherType.getPrecision();
			scale = higherType.getScale();
		}

		higherType = new DataTypeDescriptor(higherType, 
											  precision, scale, nullable, maximumWidth);

		return higherType;
	}

	/**
	 * Check whether or not the 2 types (DataTypeDescriptor) have the same type
	 * and length.
	 * This is useful for UNION when trying to decide whether a NormalizeResultSet
	 * is required.
	 *
	 * @param otherDTS	DataTypeDescriptor to compare with.
	 *
	 * @return boolean  Whether or not the 2 DTSs have the same type and length.
	 */
	public boolean isExactTypeAndLengthMatch(DataTypeDescriptor otherDTS)
	{
		/* Do both sides have the same length? */
		if (getMaximumWidth() != otherDTS.getMaximumWidth()) 
		{
			return false;
		}
		if (getScale() != otherDTS.getScale())
		{
			return false;
		}

		if (getPrecision() != otherDTS.getPrecision())
		{	
			return false;
		}

		TypeId thisType = getTypeId();
		TypeId otherType = otherDTS.getTypeId();

		/* Do both sides have the same type? */
		if ( ! thisType.equals(otherType))
		{
			return false;
		}

		return true;
	}

	/**
	* @see TypeDescriptor#getMaximumWidth
	 */
	public int	getMaximumWidth()
	{
		return typeDescriptor.getMaximumWidth();
	}

	/**
	 * Gets the TypeId for the datatype.
	 *
	 * @return	The TypeId for the datatype.
	 */
	public TypeId getTypeId()
	{
		return typeId;
	}

	/**
		Get a Null for this type.
	*/
	public DataValueDescriptor getNull() {
		return typeId.getNull();
	}

	/**
	 * Gets the name of this datatype.
	 * 
	 *
	 *  @return	the name of this datatype
	 */
	public	String		getTypeName()
	{
		return typeId.getSQLTypeName();
	}

	/**
	 * Get the jdbc type id for this type.  JDBC type can be
	 * found in java.sql.Types. 
	 *
	 * @return	a jdbc type, e.g. java.sql.Types.DECIMAL 
	 *
	 * @see Types
	 */
	public int getJDBCTypeId()
	{
		return typeId.getJDBCTypeId();
	}

	/**
	 * Returns the number of decimal digits for the datatype, if applicable.
	 *
	 * @return	The number of decimal digits for the datatype.  Returns
	 *		zero for non-numeric datatypes.
	 */
	public int	getPrecision()
	{
		return typeDescriptor.getPrecision();
	}

	/**
	 * Returns the number of digits to the right of the decimal for
	 * the datatype, if applicable.
	 *
	 * @return	The number of digits to the right of the decimal for
	 *		the datatype.  Returns zero for non-numeric datatypes.
	 */
	public int	getScale()
	{
		return typeDescriptor.getScale();
	}

	/**
	 * Returns TRUE if the datatype can contain NULL, FALSE if not.
	 * JDBC supports a return value meaning "nullability unknown" -
	 * I assume we will never have columns where the nullability is unknown.
	 *
	 * @return	TRUE if the datatype can contain NULL, FALSE if not.
	 */
	public boolean	isNullable()
	{
		return typeDescriptor.isNullable();
	}

	/**
	 * Set the nullability of the datatype described by this descriptor
	 *
	 * @param nullable	TRUE means set nullability to TRUE, FALSE
	 *			means set it to FALSE
	 *
	 * @return	Nothing
	 */
	public void	setNullability(boolean nullable)
	{
		typeDescriptor.setNullability(nullable);
	}

	/**
	  Compare if two TypeDescriptors are exactly the same
	  @param typeDescriptor the typeDescriptor to compare to.
	  */
	public boolean equals(Object aTypeDescriptor)
	{
		return typeDescriptor.equals(aTypeDescriptor);
	}

	/**
	 * Converts this data type descriptor (including length/precision)
	 * to a string. E.g.
	 *
	 *			VARCHAR(30)
	 *
	 *	or
	 *
	 *			 java.util.Hashtable 
	 *
	 * @return	String version of datatype, suitable for running through
	 *			the Parser.
	 */
	public String	getSQLstring()
	{
		return typeId.toParsableString( this );
	}

	/**
	 * Get the simplified type descriptor that is intended to be stored
	 * in the system tables.
	 */
	public TypeDescriptorImpl getCatalogType()
	{
		return typeDescriptor;
	}

	/**
	 * Get the estimated memory usage for this type descriptor.
	 */
	public double estimatedMemoryUsage() {
		switch (typeId.getTypeFormatId())
		{
			case StoredFormatIds.LONGVARBIT_TYPE_ID:
				/* Who knows?  Let's just use some big number */
				return 10000.0;

			case StoredFormatIds.BIT_TYPE_ID:
				return (double) ( ( ((float) getMaximumWidth()) / 8.0) + 0.5);

			case StoredFormatIds.BOOLEAN_TYPE_ID:
				return 4.0;

			case StoredFormatIds.CHAR_TYPE_ID:
			case StoredFormatIds.VARCHAR_TYPE_ID:
			case StoredFormatIds.NATIONAL_CHAR_TYPE_ID:
			case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID:
				return (double) (2.0 * getMaximumWidth());

			case StoredFormatIds.LONGVARCHAR_TYPE_ID:
			case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID:
				/* Who knows? Let's just use some big number */
				return 10000.0;

			case StoredFormatIds.DECIMAL_TYPE_ID:
				/*
				** 0.415 converts from number decimal digits to number of 8-bit digits. 
				** Add 1.0 for the sign byte, and 0.5 to force it to round up.
				*/
				return (double) ( (getPrecision() * 0.415) + 1.5 );

			case StoredFormatIds.DOUBLE_TYPE_ID:
				return 8.0;

			case StoredFormatIds.INT_TYPE_ID:
				return 4.0;

			case StoredFormatIds.LONGINT_TYPE_ID:
				return 8.0;

			case StoredFormatIds.REAL_TYPE_ID:
				return 4.0;

			case StoredFormatIds.SMALLINT_TYPE_ID:
				return 2.0;

			case StoredFormatIds.TINYINT_TYPE_ID:
				return 1.0;

			case StoredFormatIds.REF_TYPE_ID:
				/* I think 12 is the right number */
				return 12.0;

			case StoredFormatIds.USERDEFINED_TYPE_ID_V3:
				if (typeId.userType()) {
					/* Who knows?  Let's just use some medium-sized number */
					return 256.0;
				}
			case StoredFormatIds.DATE_TYPE_ID:
			case StoredFormatIds.TIME_TYPE_ID:
			case StoredFormatIds.TIMESTAMP_TYPE_ID:
				return 12.0; 

			default:
				return 0.0;
		}
	}

	/**
	 * Compare JdbcTypeIds to determine if they represent equivalent
	 * SQL types. For example Types.NUMERIC and Types.DECIMAL are
	 * equivalent
	 *
	 * @param existingType  JDBC type id of Cloudscape data type
	 * @param jdbcTypeIdB   JDBC type id passed in from application.
	 *
	 * @return boolean true if types are equivalent, false if not
	 */

	public static boolean isJDBCTypeEquivalent(int existingType, int jdbcTypeId)
	{
		// Any type matches itself.
		if (existingType == jdbcTypeId)
			return true;

		// To a numeric type
		if (DataTypeDescriptor.isNumericType(existingType)) {
			if (DataTypeDescriptor.isNumericType(jdbcTypeId))
				return true;

			if (DataTypeDescriptor.isCharacterType(jdbcTypeId))
				return true;

			return false;
		}

		// To character type.
		if (DataTypeDescriptor.isCharacterType(existingType)) {

			if (DataTypeDescriptor.isCharacterType(jdbcTypeId))
				return true;

			if (DataTypeDescriptor.isNumericType(jdbcTypeId))
				return true;


			switch (jdbcTypeId) {
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				return true;
			default:
				break;
			}

			
			return false;

		}

		// To binary type
		if (DataTypeDescriptor.isBinaryType(existingType)) {

			if (DataTypeDescriptor.isBinaryType(jdbcTypeId))
				return true;

			return false;
		}

		// To DATE, TIME
		if (existingType == Types.DATE || existingType == Types.TIME) {
			if (DataTypeDescriptor.isCharacterType(jdbcTypeId))
				return true;

			if (jdbcTypeId == Types.TIMESTAMP)
				return true;

			return false;
		}

		// To TIMESTAMP
		if (existingType == Types.TIMESTAMP) {
			if (DataTypeDescriptor.isCharacterType(jdbcTypeId))
				return true;

			if (jdbcTypeId == Types.DATE)
				return true;

			return false;
		}
		
		return false;
	}

	public static boolean isNumericType(int jdbcType) {

		switch (jdbcType) {
		case Types.BIT:
		case org.apache.derby.iapi.reference.JDBC30Translation.SQL_TYPES_BOOLEAN:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.REAL:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.DECIMAL:
		case Types.NUMERIC:
			return true;
		default:
			return false;
		}
	}

	private static boolean isCharacterType(int jdbcType) {

		switch (jdbcType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return true;
		default:
			return false;
		}
	}

	private static boolean isBinaryType(int jdbcType) {
		switch (jdbcType) {
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return true;
		default:
			return false;
		}
	}

	public String	toString()
	{
		return typeDescriptor.toString();
	}

	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		/* NOTE: We only write out the generic type id.
		 * typeId will be reset to be the generic type id
		 * when we get read back in since the generic
		 * one is all that is needed at execution time.
		 */
		typeId = (TypeId) in.readObject();
		typeDescriptor = (TypeDescriptorImpl) in.readObject();
	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeObject( typeId );
		out.writeObject( typeDescriptor );
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.DATA_TYPE_SERVICES_IMPL_V01_ID; }
}

