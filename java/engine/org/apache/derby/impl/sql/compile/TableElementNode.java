/*

   Derby - Class org.apache.derby.impl.sql.compile.TableElementNode

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

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A TableElementNode is an item in a TableElementList, and represents
 * a single table element such as a column or constraint in a CREATE TABLE
 * or ALTER TABLE statement.
 *
 * @author Jeff Lichtman
 */

public class TableElementNode extends QueryTreeNode
{
    /////////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////////////////

	public	static	final	int	AT_UNKNOWN						= 0;
	public	static	final	int	AT_ADD_FOREIGN_KEY_CONSTRAINT	= 1;
	public	static	final	int	AT_ADD_PRIMARY_KEY_CONSTRAINT	= 2;
	public	static	final	int	AT_ADD_UNIQUE_CONSTRAINT		= 3;
	public	static	final	int	AT_ADD_CHECK_CONSTRAINT			= 4;
	public	static	final	int	AT_DROP_CONSTRAINT				= 5;
	public	static	final	int	AT_MODIFY_COLUMN				= 6;
	public	static	final	int	AT_DROP_COLUMN					= 7;


	/////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	/////////////////////////////////////////////////////////////////////////

	String	name;
	int		elementType;	// simple element nodes can share this class,
							// eg., drop column and rename table/column/index
							// etc., no need for more classes, an effort to
							// minimize footprint

	/////////////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Initializer for a TableElementNode
	 *
	 * @param name	The name of the table element, if any
	 */

	public void init(Object name)
	{
		this.name = (String) name;
	}

	/**
	 * Initializer for a TableElementNode
	 *
	 * @param name	The name of the table element, if any
	 */

	public void init(Object name, Object elementType)
	{
		this.name = (String) name;
		this.elementType = ((Integer) elementType).intValue();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "name: " + name + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Does this element have a primary key constraint.
	 *
	 * @return boolean	Whether or not this element has a primary key constraint
	 */
	boolean hasPrimaryKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a unique key constraint.
	 *
	 * @return boolean	Whether or not this element has a unique key constraint
	 */
	boolean hasUniqueKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a foreign key constraint.
	 *
	 * @return boolean	Whether or not this element has a foreign key constraint
	 */
	boolean hasForeignKeyConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a check constraint.
	 *
	 * @return boolean	Whether or not this element has a check constraint
	 */
	boolean hasCheckConstraint()
	{
		return false;
	}

	/**
	 * Does this element have a constraint on it.
	 *
	 * @return boolean	Whether or not this element has a constraint on it
	 */
	boolean hasConstraint()
	{
		return false;
	}

	/**
	 * Get the name from this node.
	 *
	 * @return String	The name.
	 */
	public String getName()
	{
		return name;
	}

	/**
	  *	Get the type of this table element.
	  *
	  *	@return	one of the constants at the front of this file
	  */
	int	getElementType()
	{
		if ( hasForeignKeyConstraint() ) { return AT_ADD_FOREIGN_KEY_CONSTRAINT; }
		else if ( hasPrimaryKeyConstraint() ) { return AT_ADD_PRIMARY_KEY_CONSTRAINT; }
		else if ( hasUniqueKeyConstraint() ) { return AT_ADD_UNIQUE_CONSTRAINT; }
		else if ( hasCheckConstraint() ) { return AT_ADD_CHECK_CONSTRAINT; }
		else if ( this instanceof ConstraintDefinitionNode ) { return AT_DROP_CONSTRAINT; }
		else if ( this instanceof ModifyColumnNode ) { return AT_MODIFY_COLUMN; }
		else { return elementType; }
	}

}
