/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.UUID;

/**
 * A TableKey represents a immutable unique identifier for a SQL object.
 * It has a schemaid and a name	. 
 *
 * @author Jamie -- lifed from Comp/TableName
 */

final class TableKey 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	private final String	tableName;
	private final UUID	schemaId;


	/**
	 * Constructor for when you have both the table and schema names.
	 *
	 * @param schemaId		The UUID of the schema being referecned
	 * @param tableName		The name of the table being referenced	 
	 */
	TableKey(UUID schemaUUID, String tableName)
	{
		this.tableName = tableName;
		this.schemaId = schemaUUID;
	}

	/**
	 * Get the table name (without the schema name).
	 *
	 * @return Table name as a String
	 */

	String getTableName()
	{
		return tableName;
	}

	/**
	 * Get the schema id.
	 *
	 * @return Schema id as a String
	 */

	UUID getSchemaId()
	{
		return schemaId;
	}

	/**
	 * 2 TableKeys are equal if their both their schemaIds and tableNames are
	 * equal.
	 *
	 * @param otherTableKey	The other TableKey, as Object.
	 *
	 * @return boolean		Whether or not the 2 TableKey are equal.
	 */
	public boolean equals(Object otherTableKey)
	{
		if (otherTableKey instanceof TableKey) {

			TableKey otk = (TableKey) otherTableKey;
			if (tableName.equals(otk.tableName) && schemaId.equals(otk.schemaId))
				return true;
		}
		return false;
	}

	public int hashCode()
	{
		return tableName.hashCode();
	}

}
