/*

   Derby - Class org.apache.derby.impl.sql.compile.CastNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.DataTypeUtilities;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.VariableSizeDataValue;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.JDBC30Translation;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import java.lang.reflect.Modifier;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.types.NumberDataType;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.ReuseFactory;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.types.SQLReal;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.util.Vector;

/**
 * An CastNode represents a cast expressionr.
 *
 * @author Jerry Brenner
 */

public class CastNode extends ValueNode
{
	DataTypeDescriptor	castTarget;
	ValueNode			castOperand;
	int					targetCharType;
	TypeId	destCTI = null;
	TypeId	sourceCTI = null;
	boolean forDataTypeFunction = false;

	/*
	** Static array of valid casts.  Dimentions
	** produce a single boolean which indicates
	** whether the case is possible or not.
	*/

	/**
	 * Initializer for a CastNode
	 *
	 * @param castOperand	The operand of the node
	 * @param castTarget	DataTypeServices (target type of cast)
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object castOperand, Object castTarget)
		throws StandardException
	{
		this.castOperand = (ValueNode) castOperand;
		this.castTarget = (DataTypeDescriptor) castTarget;
	}

	/**
	 * Initializer for a CastNode
	 *
	 * @param castOperand	The operand of the node
	 * @param charType		CHAR or VARCHAR JDBC type as target
	 * @param charLength	target type length
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object castOperand, Object charType, Object charLength)
		throws StandardException
	{
		this.castOperand = (ValueNode) castOperand;
		int charLen = ((Integer) charLength).intValue();
		targetCharType = ((Integer) charType).intValue();
		if (charLen < 0)	// unknown, figure out later
			return;
		this.castTarget = DataTypeDescriptor.getBuiltInDataTypeDescriptor(targetCharType, charLen);
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return		This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "castTarget: " + castTarget + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (castOperand != null)
			{
				printLabel(depth, "castOperand: ");
				castOperand.treePrint(depth + 1);
			}
		}
	}
	protected int getOrderableVariantType() throws StandardException
	{
		return castOperand.getOrderableVariantType();
	}

	/**
	 * Set the clause that this node appears in.
	 *
	 * @param clause	The clause that this node appears in.
	 *
	 * @return Nothing.
	 */
	public void setClause(int clause)
	{
		super.setClause(clause);
		castOperand.setClause(clause);
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
									Vector aggregateVector)
				throws StandardException
	{
		castOperand = castOperand.bindExpression(
								fromList, subqueryList,
								aggregateVector);

		if (castTarget == null)   //CHAR or VARCHAR function without specifying target length
		{
			DataTypeDescriptor opndType = castOperand.getTypeServices();
			int length = -1;
			TypeId srcTypeId = opndType.getTypeId();
			if (opndType != null)
			{
				if (srcTypeId.isNumericTypeId())
				{
					length = opndType.getPrecision() + 1; // 1 for the sign
					if (opndType.getScale() > 0)
						length += 1;               // 1 for the decimal .
				 
				}
				else 
				{
					TypeId typeid = opndType.getTypeId();
					if (length < 0)
						length = DataTypeUtilities.getColumnDisplaySize(typeid.getJDBCTypeId(),-1);

				}
			}
			if (length < 0)
				length = 1;  // same default as in parser
			castTarget = DataTypeDescriptor.getBuiltInDataTypeDescriptor(targetCharType, length);
			
		}

		/* 
		** If castOperand is an untyped null, 
		** then we must set the type.
		*/
		if (castOperand instanceof UntypedNullConstantNode)
		{
			castOperand.setType(castTarget);
		}

		bindCastNodeOnly();
		
		/* We can't chop out cast above an untyped null because
		 * the store can't handle it.
		 */
		if ((castOperand instanceof ConstantNode) &&
			!(castOperand instanceof UntypedNullConstantNode))
		{
			/* If the castOperand is a typed constant then we do the cast at
			 * bind time and return a constant of the correct type.
			 * NOTE: This could return an exception, but we're prepared to 
			 * deal with that. (NumberFormatException, etc.)
			 * We only worry about the easy (and useful)
			 * converions at bind time.
			 * Here's what we support:
			 *			source					destination
			 *			------					-----------
			 *			boolean					boolean
			 *			boolean					char
			 *			char					boolean
			 *			char					date/time/ts
			 *			char					non-decimal numeric
			 *			date/time/ts			char
			 *			numeric					char
			 *			numeric					non-decimal numeric
			 */
			/* RESOLVE - to be filled in. */
			ValueNode retNode = this;
			int		  sourceJDBCTypeId = sourceCTI.getJDBCTypeId();
			int		  destJDBCTypeId = destCTI.getJDBCTypeId();

			switch (sourceJDBCTypeId)
			{
				case Types.BIT:
				case JDBC30Translation.SQL_TYPES_BOOLEAN:
					// (BIT is boolean)
					if (destJDBCTypeId == Types.BIT || destJDBCTypeId == JDBC30Translation.SQL_TYPES_BOOLEAN)
					{
						retNode = castOperand;
					}
					else if (destJDBCTypeId == Types.CHAR)
					{
						BooleanConstantNode bcn = (BooleanConstantNode) castOperand;
						String booleanString = bcn.getValueAsString();
						retNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.CHAR_CONSTANT_NODE,
											booleanString,
											ReuseFactory.getInteger(
												castTarget.getMaximumWidth()),
											getContextManager());
					}
					break;

					case Types.CHAR:
						retNode = getCastFromCharConstant(destJDBCTypeId);
						break;

					case Types.DATE:
					case Types.TIME:
					case Types.TIMESTAMP:
						if (destJDBCTypeId == Types.CHAR)
						{
							String castValue =  
								((UserTypeConstantNode) castOperand).
											getObjectValue().
												toString();
							retNode = (ValueNode) getNodeFactory().getNode(
												C_NodeTypes.CHAR_CONSTANT_NODE,
												castValue, 
												ReuseFactory.getInteger(
												  castTarget.getMaximumWidth()),
												getContextManager());
						}
						break;

					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
						long longValue = ((NumericConstantNode) castOperand).getLong();
						retNode = getCastFromIntegralType(
											longValue, 
											destJDBCTypeId);
						break;

					case Types.DOUBLE:
					case Types.REAL:
						double doubleValue = ((NumericConstantNode) castOperand).getDouble();
						retNode = getCastFromNonIntegralType(
											doubleValue, 
											destJDBCTypeId);
						break;

					case Types.DECIMAL:
						// ignore decimal -> decimal casts for now
						if (destJDBCTypeId != Types.DECIMAL &&
							destJDBCTypeId != Types.NUMERIC)
						{
							/* SQLDecimal.getDouble() throws an exception if the
							 * BigDecimal is outside of the range of double.
							 */
							doubleValue = ((ConstantNode) castOperand).getValue().getDouble();
							retNode = getCastFromNonIntegralType(
												doubleValue, 
												destJDBCTypeId);
						}
						break;
			}

			// Return the new constant if the cast was performed
			return retNode;
		}

		return this;
	}

	/**
	 * Bind this node but not its child.  Caller has already bound
	 * the child.
	 * This is useful for when we generate a CastNode during binding
	 * after having already bound the child.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindCastNodeOnly()
		throws StandardException
	{

		/*
		** The result type is always castTarget.
		*/
		setType(castTarget);
		destCTI = castTarget.getTypeId();
		sourceCTI = castOperand.getTypeId();

		/* 
		** If it is a java cast, do some work to make sure
		** the classes are ok and that they are compatible
		*/
		if (! destCTI.systemBuiltIn())
		{
			String className = ((TypeId) dataTypeServices.getTypeId()).getCorrespondingJavaTypeName();

			boolean convertCase = ! destCTI.getClassNameWasDelimitedIdentifier();

			className = verifyClassExist(className, convertCase);

			castTarget = new DataTypeDescriptor(TypeId.getUserDefinedTypeId(className, false),
														true /* assume nullable for now, change it if not nullable */
													);
			setType(castTarget);
			destCTI = castTarget.getTypeId();
		}

		if (castOperand.isParameterNode())
		{
			bindParameter();
		}

		/*
		** If it isn't null, then we have
		** a cast from one JBMS type to another.  So we
		** have to figure out if it is legit.
		*/
		else if (!(castOperand instanceof UntypedNullConstantNode))
		{
			/*
			** Make sure we can assign the two classes
			*/
			TypeCompiler tc = castOperand.getTypeCompiler();
			if (! tc.convertible(destCTI, forDataTypeFunction))
			{
				throw StandardException.newException(SQLState.LANG_INVALID_CAST, 
						sourceCTI.getSQLTypeName(),
						destCTI.getSQLTypeName());
			}
		}		
	}

	/**
	 * Get a constant representing the cast from a CHAR to another
	 * type.  If this is not an "easy" cast to perform, then just
	 * return this cast node.
	 * Here's what we think is "easy":
	 *			source			destination
	 *			------			-----------
	 *			char			boolean
	 *			char			date/time/ts
	 *			char			non-decimal numeric
	 *
	 * @param destJDBCTypeId	The destination JDBC TypeId
	 *
	 * @return The new top of the tree (this CastNode or a new Constant)
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode getCastFromCharConstant(int destJDBCTypeId)
		throws StandardException
	{
		String	  charValue = ((CharConstantNode) castOperand).getString();
		String	  cleanCharValue = StringUtil.SQLToUpperCase(charValue.trim());
		ValueNode retNode = this;

		switch (destJDBCTypeId)
		{
			case Types.BIT:
			case JDBC30Translation.SQL_TYPES_BOOLEAN:
				if (cleanCharValue.equals("TRUE"))
				{
					return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());
				}
				else if (cleanCharValue.equals("FALSE"))
				{
					return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.FALSE,
											getContextManager());
				}
				else
				{
					throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "boolean");
				}

			case Types.DATE:
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.USERTYPE_CONSTANT_NODE,
										getDataValueFactory().getDateValue(cleanCharValue, false),
										getContextManager());

			case Types.TIMESTAMP:
				return (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.USERTYPE_CONSTANT_NODE,
									getDataValueFactory().getTimestampValue(cleanCharValue, false),
									getContextManager());

			case Types.TIME:
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.USERTYPE_CONSTANT_NODE,
										getDataValueFactory().getTimeValue(cleanCharValue, false),
										getContextManager());

			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:				
				try 
				{
					// #3756 - Truncate decimal portion for casts to integer
					return getCastFromIntegralType((new Double(cleanCharValue)).longValue(),
												   destJDBCTypeId);
				}
				catch (NumberFormatException nfe)
				{
					String sqlName = TypeId.getBuiltInTypeId(destJDBCTypeId).getSQLTypeName();
					throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, sqlName);
				}
			case Types.REAL:
				Float floatValue;
				try
				{
					floatValue = Float.valueOf(cleanCharValue);
				}
				catch (NumberFormatException nfe)
				{
					throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "float");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.FLOAT_CONSTANT_NODE,
											floatValue,
											getContextManager());
			case Types.DOUBLE:
				Double doubleValue;
				try
				{
					doubleValue = new Double(cleanCharValue);
				}
				catch (NumberFormatException nfe)
				{
					throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, "double");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.DOUBLE_CONSTANT_NODE,
											doubleValue,
											getContextManager());
		}

		return retNode;
	}


	/**
	 * Get a constant representing the cast from an integral type to another
	 * type.  If this is not an "easy" cast to perform, then just
	 * return this cast node.
	 * Here's what we think is "easy":
	 *			source				destination
	 *			------				-----------
	 *			integral type		 non-decimal numeric
	 *			integral type		 char
	 *
	 * @param longValue			integral type as a long to cast from
	 * @param destJDBCTypeId	The destination JDBC TypeId
	 *
	 * @return The new top of the tree (this CastNode or a new Constant)
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode getCastFromIntegralType(
									  long longValue, 
									  int destJDBCTypeId)
		throws StandardException
	{
		ValueNode retNode = this;

		switch (destJDBCTypeId)
		{
			case Types.CHAR:
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.CHAR_CONSTANT_NODE,
										Long.toString(longValue), 
										ReuseFactory.getInteger(
											castTarget.getMaximumWidth()),
										getContextManager());
			case Types.TINYINT:
				if (longValue < Byte.MIN_VALUE ||
					longValue > Byte.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
				}
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.TINYINT_CONSTANT_NODE,
										ReuseFactory.getByte((byte) longValue),
										getContextManager());

			case Types.SMALLINT:
				if (longValue < Short.MIN_VALUE ||
					longValue > Short.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SHORT");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.SMALLINT_CONSTANT_NODE,
											ReuseFactory.getShort(
															(short) longValue),
											getContextManager());

			case Types.INTEGER:
				if (longValue < Integer.MIN_VALUE ||
					longValue > Integer.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
				}
				return (ValueNode) getNodeFactory().getNode(
												C_NodeTypes.INT_CONSTANT_NODE,
												ReuseFactory.getInteger(
															(int) longValue),
												getContextManager());

			case Types.BIGINT:
				return (ValueNode) getNodeFactory().getNode(
								C_NodeTypes.LONGINT_CONSTANT_NODE,
								ReuseFactory.getLong(longValue),
								getContextManager());

			case Types.REAL:
				if (Math.abs(longValue) > Float.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "REAL");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.FLOAT_CONSTANT_NODE,
											new Float((float) longValue),
											getContextManager());

			case Types.DOUBLE:
				return (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.DOUBLE_CONSTANT_NODE,
									new Double((double) longValue),
									getContextManager());
		}

		return retNode;
	}

	/**
	 * Get a constant representing the cast from a non-integral type to another
	 * type.  If this is not an "easy" cast to perform, then just
	 * return this cast node.
	 * Here's what we think is "easy":
	 *			source				destination
	 *			------				-----------
	 *			non-integral type	 non-decimal numeric
	 *			non-integral type	 char
	 *
	 * @param doubleValue		non-integral type a a double to cast from
	 * @param destJDBCTypeId	The destination JDBC TypeId
	 *
	 * @return The new top of the tree (this CastNode or a new Constant)
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode getCastFromNonIntegralType(
									  double doubleValue, 
									  int destJDBCTypeId)
		throws StandardException
	{
		String	  stringValue = null;
		ValueNode retNode = this;

		switch (destJDBCTypeId)
		{
			case Types.CHAR:
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.CHAR_CONSTANT_NODE,
										Double.toString(doubleValue), 
										ReuseFactory.getInteger(
											castTarget.getMaximumWidth()),
										getContextManager());
			case Types.TINYINT:
				doubleValue = Math.floor(doubleValue);
				if (doubleValue < Byte.MIN_VALUE ||
					doubleValue > Byte.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "TINYINT");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.TINYINT_CONSTANT_NODE,
											ReuseFactory.getByte(
															(byte) doubleValue),
											getContextManager());

			case Types.SMALLINT:
				doubleValue = Math.floor(doubleValue);
				if (doubleValue < Short.MIN_VALUE ||
					doubleValue > Short.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
				}
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.SMALLINT_CONSTANT_NODE,
											ReuseFactory.getShort(
														(short) doubleValue),
											getContextManager());

			case Types.INTEGER:
				doubleValue = Math.floor(doubleValue);
				if (doubleValue < Integer.MIN_VALUE ||
					doubleValue > Integer.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "INTEGER");
				}
				return (ValueNode) getNodeFactory().getNode(
												C_NodeTypes.INT_CONSTANT_NODE,
												ReuseFactory.getInteger(
															(int) doubleValue),
												getContextManager());

			case Types.BIGINT:
				doubleValue = Math.floor(doubleValue);
				if (doubleValue < Long.MIN_VALUE ||
					doubleValue > Long.MAX_VALUE)
				{
					throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "BIGINT");
				}
				return (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.LONGINT_CONSTANT_NODE,
									ReuseFactory.getLong((long) doubleValue),
									getContextManager());

			case Types.REAL:
//                System.out.println("cast to real!");
//				if (Math.abs(doubleValue) > Float.MAX_VALUE)
//                    throw...
//                SQLReal.check(doubleValue);
// jsk: rounding problem???
				return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.FLOAT_CONSTANT_NODE,
											new Float(NumberDataType.normalizeREAL(doubleValue)),
											getContextManager());

			case Types.DOUBLE:
				return (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.DOUBLE_CONSTANT_NODE,
										new Double(doubleValue),
										getContextManager());
		}

		return retNode;
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		castOperand = castOperand.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);
		return this;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		return castOperand.categorize(referencedTabs, simplePredsOnly);
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		castOperand = castOperand.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return castOperand.isConstantExpression();
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return castOperand.constantExpression(whereClause);
	}

	/**
	 * By default unary operators don't accept ? parameters as operands.
	 * This can be over-ridden for particular unary operators.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Always thrown to indicate a
	 *									? parameter where it isn't allowed.
	 */

	void bindParameter()
					throws StandardException
	{
		((ParameterNode) castOperand).setDescriptor(castTarget);
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
		Object sourceObject = castOperand.getConstantValueAsObject();

		// RESOLVE - need to figure out how to handle casts
		if (sourceObject == null)
		{
			return null;
		}

		// Simple if source and destination are of same type
		if (sourceCTI.getCorrespondingJavaTypeName().equals(
				destCTI.getCorrespondingJavaTypeName()))
		{
			return sourceObject;
		}

		// RESOLVE - simply return null until we can figure out how to 
		// do the cast
		return null;
	}

	/**
	 * Do code generation for this unary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 * @return	An expression to evaluate this operator
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		castOperand.generateExpression(acb, mb);

		/* No need to generate code for null constants */
		if (castOperand instanceof UntypedNullConstantNode)
		{
			return;
		}
		/* HACK ALERT. When casting a parameter, there
		 * is not sourceCTI.  Code generation requires one,
		 * so we simply set it to be the same as the
		 * destCTI.  The user can still pass whatever
		 * type they'd like in as a parameter.
		 * They'll get an exception, as expected, if the
		 * conversion cannot be performed.
		 */
		else if (castOperand.isParameterNode())
		{
			sourceCTI = destCTI;
		}
	
		genDataValueConversion(acb, mb);
	}

	private void genDataValueConversion(ExpressionClassBuilder acb,
											  MethodBuilder mb)
			throws StandardException
	{
		MethodBuilder	acbConstructor = acb.getConstructor();

		String resultTypeName = getTypeCompiler().interfaceName();

		/* field = method call */
		/* Allocate an object for re-use to hold the result of the operator */
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

		/*
		** Store the result of the method call in the field, so we can re-use
		** the object.
		*/

		acb.generateNull(acbConstructor, getTypeCompiler(destCTI));
		acbConstructor.putField(field);
		acbConstructor.endStatement();



		/*
			For most types generate

			targetDVD.setValue(sourceDVD);

			// optional for variable length types
			targetDVD.setWidth();
		*/

		if (!sourceCTI.isNationalStringTypeId() && !sourceCTI.userType() && !destCTI.userType()) {
		mb.getField(field); // targetDVD reference for the setValue method call
		mb.swap();
		mb.cast(ClassName.DataValueDescriptor);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.DataValueDescriptor, "setValue", "void", 1);

		mb.getField(field);
		/* 
		** If we are casting to a variable length datatype, we
		** have to make sure we have set it to the correct
		** length.
		*/
		if (destCTI.variableLength()) 
		{
			boolean isNumber = destCTI.isNumericTypeId();

			/* setWidth() is on VSDV - upcast since
			 * decimal implements subinterface
			 * of VSDV.
			 */
			mb.push(isNumber ? castTarget.getPrecision() : castTarget.getMaximumWidth());
			mb.push(castTarget.getScale());
			mb.push(!sourceCTI.variableLength() || isNumber);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", ClassName.DataValueDescriptor, 3);

			/* setWidth returns DataValueDescriptor - we need
 			 * to cast result to actual subinterface getting returned.
			 */
			mb.cast(resultTypeName);
		}

		return;
		}






		/*
		** If we are casting from a national string to a date, time,
		** or timestamp, do a getDate(), getTime(), or getTimestamp()
		** rather than a getObject(). This is because getObject() returns
		** a String, and setValue() can't tell whether the String comes
		** from a national or non-national character type, so it can't tell
		** whether to use the database locale to do the conversion.
		*/
		String getMethod = "getObject";
		String getType = "java.lang.Object";
		String castType = sourceCTI.getCorrespondingJavaTypeName();
		int argCount = 0;
		if (sourceCTI.isNationalStringTypeId())
		{
			switch (destCTI.getJDBCTypeId())
			{
			  case Types.DATE:
				getMethod = "getDate";
				getType = "java.sql.Date";
				castType = getType;
				break;

			  case Types.TIME:
				getMethod = "getTime";
				getType = "java.sql.Time";
				castType = getType;
				break;

			  case Types.TIMESTAMP:
				getMethod = "getTimestamp";
				getType = "java.sql.Timestamp";
				castType = getType;
				break;
			}

			if (!getMethod.equals("getObject")) {

				mb.pushThis();
				mb.callMethod(VMOpcode.INVOKEVIRTUAL, 
					acb.getBaseClassName(), 
					"getCalendar", "java.util.Calendar", 0);

				argCount++;
			}
		}

		/* 
		** generate: field.setValue((<type>) expr.getObject ) 
		** or		 field.setValue((<type>) expr.getDate )
		** or		 field.setValue((<type>) expr.getTime )
		** or		 field.setValue((<type>) expr.getTimestamp )
		*/
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.DataValueDescriptor, getMethod, getType, argCount);

		/* 
		** Cast to java.lang.Object if source or destination type 
		** is a java type because that's how the interface is defined.
		*/
		mb.cast(destCTI.userType() || sourceCTI.userType() ? "java.lang.Object" : castType);
		//castExpr

		mb.getField(field); // instance for the setValue/setObjectForCast method call
		mb.swap(); // push it before the value

		/*
		** If we are casting a java type, then
		** we generate:
		**
		**		DataValueDescriptor.setObjectForCast(java.lang.Object castExpr, boolean instanceOfExpr, destinationClassName)
		** where instanceOfExpr is "source instanceof destinationClass".
		**
		** otherwise:
		**
		**		<specificDataValue>.setValue(<type>castExpr)
		*/
		if (sourceCTI.userType())
		{

			String destinationType = getTypeId().getCorrespondingJavaTypeName();

			// at this point method instance and cast result are on the stack
			// we duplicate the cast value in order to perform the instanceof check
			mb.dup();
			mb.isInstanceOf(destinationType);
			mb.push(destinationType);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.DataValueDescriptor, "setObjectForCast", "void", 3);
		}
		else
		{
			String itype = ClassName.DataValueDescriptor;
			if (castType.startsWith("java.lang.")) {
				if (!castType.equals("java.lang.String") && !castType.equals("java.lang.Object"))
					itype = resultTypeName;
			}
			// System.out.println("type = " + castType);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, itype, "setValue", "void", 1);
			// mb.endStatement();
		}
		mb.getField(field);

		/* 
		** If we are casting to a variable length datatype, we
		** have to make sure we have set it to the correct
		** length.
		*/
		if (destCTI.variableLength()) 
		{
			boolean isNumber = destCTI.isNumericTypeId();

			/* setWidth() is on VSDV - upcast since
			 * decimal implements subinterface
			 * of VSDV.
			 */
			mb.push(isNumber ? castTarget.getPrecision() : castTarget.getMaximumWidth());
			mb.push(castTarget.getScale());
			mb.push(!sourceCTI.variableLength() || isNumber);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", ClassName.DataValueDescriptor, 3);

								/*
								** The last argument is true if we should
								** throw error on truncation.  We throw an
								** error on all but Bits and Strings
								** (everything with variable length that
								** isn't a number -- all variable length
								** except DECIMAL/NUMERIC).
								*/
								/* RESOLVE:
								** NOTE: If the source is a parameter
								** then the user can pass any type
								** in as the parameter.  We will not
								** raise a truncation exception in
								** this case, even if we would if the
								** cast was directly on the value
								** being passed in as a parameter.
								** For example:
								**	cast(123 as char(1)) throws truncation
								**			exception
								**	cast(? as char(1)), user passes 123
								**		no truncation exception
								** We are considering this behavior to be
								** an extension, at least for now. We may 
								** need to revisit this if there's a
								** SQL-J compliance test with this.
								** (The solution would be to add a method
								** off of ParameterValueSet to get some
								** info about the data type of the
								** actual parameter and generate code for
								** the 3rd parameter to setWidth() based
								** on the execution time data type.
								*/
			/* setWidth returns DataValueDescriptor - we need
 			 * to cast result to actual subinterface getting returned.
			 */
			mb.cast(resultTypeName);
		}
	
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		Visitable returnNode = v.visit(this);
	
		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (castOperand != null && !v.stopTraversal())
		{
			castOperand = (ValueNode)castOperand.accept(v);
		}

		return returnNode;
	}

	/** set this to be a dataTypeScalarFunction
	 * 
	 * @param b true to use function conversion rules
	 */
	public void setForDataTypeFunction(boolean b)
	{
		forDataTypeFunction = b;
	}

	/** is this a cast node for a data type scalar function?
	 * @return true if this is  a function, false for regular cast node
	 *
	 */
	public boolean getForDataTypeFunction()
	{
		return forDataTypeFunction;
	}
}


