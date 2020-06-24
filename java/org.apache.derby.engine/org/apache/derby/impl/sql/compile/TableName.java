/*

   Derby - Class org.apache.derby.impl.sql.compile.TableName

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.util.IdUtil;

/**
 * A TableName represents a qualified name, externally represented as a schema name
 * and an object name separated by a dot. This class is misnamed: it is used to
 * represent the names of other object types in addition to tables.
 *
 */

public class TableName extends QueryTreeNode
{
	/* Both schemaName and tableName can be null, however, if 
	** tableName is null then schemaName must also be null.
	*/
	String	tableName;
	String	schemaName;
	private boolean hasSchema;

	/**
     * Constructor for when we have both the table and schema names.
	 *
	 * @param schemaName	The name of the schema being referenced
     * @param tableName     The name of the table or other object being
     *                      referenced
     * @param cm            The context manager
	 */

    TableName(String schemaName, String tableName, ContextManager cm)
	{
        super(cm);
//IC see: https://issues.apache.org/jira/browse/DERBY-18
		hasSchema = schemaName != null;
        this.schemaName = schemaName;
        this.tableName = tableName;
	}

	/**
     * Constructor for when we have both the table and schema names.
	 *
	 * @param schemaName	The name of the schema being referenced
     * @param tableName     The name of the table or other object being
     *                      referenced
	 * @param tokBeginOffset begin position of token for the table name 
	 *					identifier from parser.  pass in -1 if unknown
	 * @param tokEndOffset	end position of token for the table name 
	 *					identifier from parser.  pass in -1 if unknown
     * @param cm            The context manager
	 */
    TableName(
        String schemaName,
        String tableName,
        int tokBeginOffset,
        int tokEndOffset,
        ContextManager cm)
	{
        this(schemaName, tableName, cm);
        this.setBeginOffset(tokBeginOffset);
        this.setEndOffset(tokEndOffset);
	}

	/**
	 * Get the table name (without the schema name).
	 *
	 * @return Table name as a String
	 */

	public String getTableName()
	{
		return tableName;
	}

	/**
	 * Return true if this instance was initialized with not null schemaName.
	 *
	 * @return true if this instance was initialized with not null schemaName
	 */
    public boolean hasSchema() {
		return hasSchema;
	}

	/**
	 * Get the schema name.
	 *
	 * @return Schema name as a String
	 */

	public String getSchemaName()
	{
		return schemaName;
	}

	/**
	 * Set the schema name.
	 *
	 * @param schemaName	 Schema name as a String
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setSchemaName(String schemaName)
	{
		this.schemaName = schemaName;
	}

	/**
	 * Get the full table name (with the schema name, if explicitly
	 * specified).
	 *
	 * @return Full table name as a String
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getFullTableName()
	{
		if (schemaName != null)
			return schemaName + "." + tableName;
		else
			return tableName;
	}

    /**
     * Get the full SQL name of this object, properly quoted and escaped.
     */
    public  String  getFullSQLName()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        return IdUtil.mkQualifiedName( schemaName, tableName );
    }

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-18
		if (hasSchema)
			return getFullTableName();
		else
			return tableName;
	}

	/**
	 * 2 TableNames are equal if their both their schemaNames and tableNames are
	 * equal, or if this node's full table name is null (which happens when a
	 * SELECT * is expanded).  Also, only check table names if the schema
	 * name(s) are null.
	 *
	 * @param otherTableName	The other TableName.
	 *
	 * @return boolean		Whether or not the 2 TableNames are equal.
	 */
    boolean equals(TableName otherTableName)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-127
        if( otherTableName == null)
            return false;
        
		String fullTableName = getFullTableName();
		if (fullTableName == null)
		{
			return true;
		}
		else if ((schemaName == null) || 
				 (otherTableName.getSchemaName() == null))
		{
			return tableName.equals(otherTableName.getTableName());
		}
		else
		{
			return fullTableName.equals(otherTableName.getFullTableName());
		}
	}

	/**
	 * 2 TableNames are equal if their both their schemaNames and tableNames are
	 * equal, or if this node's full table name is null (which happens when a
	 * SELECT * is expanded).  Also, only check table names if the schema
	 * name(s) are null.
	 *
	 * @param otherSchemaName	The other TableName.
	 * @param otherTableName	The other TableName.
	 *
	 * @return boolean		Whether or not the 2 TableNames are equal.
	 */
    boolean equals(String otherSchemaName, String otherTableName)
	{
		String fullTableName = getFullTableName();
		if (fullTableName == null)
		{
			return true;
		}
		else if ((schemaName == null) || 
				 (otherSchemaName == null))
		{
			return tableName.equals(otherTableName);
		}
		else
		{
			return fullTableName.equals(otherSchemaName+"."+otherTableName);
		}
	}

    /** Clone this TableName */
    public  TableName   cloneMe()
    {
        return new TableName( schemaName, tableName, getContextManager() );
    }

	///////////////////////////////////////////////////////////////////////
	//
	//	BIND METHODS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Bind this TableName. This means filling in the schema name if it
	  *	wasn't specified.
	  *
	  *	@exception StandardException		Thrown on error
	  */
    void bind() throws StandardException
	{
        schemaName = getSchemaDescriptor(schemaName).getSchemaName();
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	OBJECT INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
      * Returns a hash code for this tableName. This allows us to use TableNames
	  *	as keys in hash lists.
	  *
      * @return hash code for this table name
	  */
    @Override
	public	int	hashCode()
	{
		return	getFullTableName().hashCode();
	}

	/**
	  *	Compares two TableNames. Needed for hashing logic to work.
	  *
	  *	@param	other	other tableName
	  */
    @Override
	public	boolean	equals( Object other )
	{
		if ( !( other instanceof TableName ) ) { return false; }

		TableName		that = (TableName) other;

		return	this.getFullTableName().equals( that.getFullTableName() );
	}

}
