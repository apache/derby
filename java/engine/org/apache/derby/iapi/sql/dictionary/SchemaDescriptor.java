/* IBM Confidential
 *
 * Product ID: 5697-F53
 *

 * Copyright 1997, 2004.WESTHAM

 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Dependable;

/**
 * This class represents a schema descriptor
 *
 * @version 0.1
 * @author Jeff Lichtman
 */

public class SchemaDescriptor extends TupleDescriptor 
	implements UniqueTupleDescriptor, Provider
{
	
	/*
	** When we boot, we put the system tables in
	** in 'SYS', owned by user DBA.
	** '92 talks about two system schemas:
	**
	**		Information Schema: literal name is 
	**		SYS.  This schema contains
	**		a series of well defined views that reference
	**		actual base tables from the Definition Schema.
	**
	**		Definition Schema:  literal name is
	**		DEFINITION_SCHEMA.  This schema contains 
	** 		system tables that can be in any shape or
	**		form.
	**	
	** SYS is owned by SA_USER_NAME (or DBA).
	*/
    /**
     * STD_SYSTEM_SCHEMA_NAME is the name of the system schema in databases that
     * use ANSI standard identifier casing. In LSA and in Cloudscape target 
     * databases the name will use the same case as the source database 
     * identifiers.
     *
     * @see org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext#getSystemSchemaName
     */
    public static final	String	STD_SYSTEM_SCHEMA_NAME      = "SYS";

    public static final	String	IBM_SYSTEM_SCHEMA_NAME      = "SYSIBM";

    /*
     * Names of system schemas.
     * The following schemas exist in a standard empty DB2 database.  For
     * now creating them in the cloudscape database but not actually putting
     * any objects in them.  Users should not be able to create any objects
     * in these schemas.
     **/
    public static final	String	IBM_SYSTEM_CAT_SCHEMA_NAME      = "SYSCAT";
    public static final	String	IBM_SYSTEM_FUN_SCHEMA_NAME      = "SYSFUN";
    public static final	String	IBM_SYSTEM_PROC_SCHEMA_NAME     = "SYSPROC";
    public static final	String	IBM_SYSTEM_STAT_SCHEMA_NAME     = "SYSSTAT";
    public static final	String	IBM_SYSTEM_NULLID_SCHEMA_NAME   = "NULLID";

    /**
     * This schema is used for jar handling procedures.
     **/
    public static final	String	STD_SQLJ_SCHEMA_NAME      = "SQLJ";
     
    /**
     * This schema is for cloudscape specific system diagnostic procedures and 
     * functions which are not available in DB2.  
     **/
    public static final	String	STD_SYSTEM_DIAG_SCHEMA_NAME     = "SYSCS_DIAG";

    /**
     * This schema is for cloudscape specific system diagnostic procedures and 
     * functions which are not available in DB2.  
     **/
    public static final	String	STD_SYSTEM_UTIL_SCHEMA_NAME     = "SYSCS_UTIL";

    /**
     * STD_DEFAULT_SCHEMA_NAME is the name of the default schema in databases 
     * that use ANSI standard identifier casing. In LSA and in Cloudscape 
     * target databases the name will use the same case as the source database 
     * identifiers.
     *
     * @see org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext#getDefaultSchemaName
     */
    public	static	final	String	STD_DEFAULT_SCHEMA_NAME = Property.DEFAULT_USER_NAME;


    /**
     * UUID's used as key's in the SYSSCHEMA catalog for the system schema's
     **/
    public static final String SYSCAT_SCHEMA_UUID      =  
        "c013800d-00fb-2641-07ec-000000134f30";
    public static final String SYSFUN_SCHEMA_UUID      =  
        "c013800d-00fb-2642-07ec-000000134f30";
    public static final String SYSPROC_SCHEMA_UUID     =  
        "c013800d-00fb-2643-07ec-000000134f30";
    public static final String SYSSTAT_SCHEMA_UUID     =  
        "c013800d-00fb-2644-07ec-000000134f30";
    public static final String SYSCS_DIAG_SCHEMA_UUID  =  
        "c013800d-00fb-2646-07ec-000000134f30";
    public static final String SYSCS_UTIL_SCHEMA_UUID  =  
        "c013800d-00fb-2649-07ec-000000134f30";
    public static final String NULLID_SCHEMA_UUID  =  
        "c013800d-00fb-2647-07ec-000000134f30";
    public static final String SQLJ_SCHEMA_UUID  =  
        "c013800d-00fb-2648-07ec-000000134f30";
	public static final	String SYSTEM_SCHEMA_UUID =  
        "8000000d-00d0-fd77-3ed8-000a0a0b1900";
	public static final	String SYSIBM_SCHEMA_UUID =  
        "c013800d-00f8-5b53-28a9-00000019ed88";
	public static final	String DEFAULT_SCHEMA_UUID = 
        "80000000-00d2-b38f-4cda-000a0a412c00";



    public	static	final	String	STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME = "SESSION";
	public	static	final	String	DEFAULT_USER_NAME = Property.DEFAULT_USER_NAME;
	public	static	final	String	SA_USER_NAME = "DBA";


	/** the public interface for this system:
		<ol>
		<li>public String getSchemaName();
		<li>public String getAuthorizationId();
		<li>public void	setUUID(UUID uuid);
		<li>public boolean isSystemSchema();
		</ol>
	*/

	//// Implementation
	private final String			name;
	private UUID			oid;
	private final String			aid;

    private boolean isSystem;
    private boolean isSYSIBM;

	/**
	 * Constructor for a SchemaDescriptor.
	 *
     * @param dataDictionary
	 * @param name	        The schema descriptor for this table.
	 * @param oid	        The object id
	 * @param aid	        The authorizatin id
     * @param isSystem	    boolean, true iff this is a system schema, like SYS,
     *                      SYSIBM, SYSCAT, SYSFUN, ....
	 */
	public SchemaDescriptor(
    DataDictionary  dataDictionary, 
    String          name,
    String          aid, 
    UUID            oid,
    boolean         isSystem)
	{
		super (dataDictionary);

		this.name = name;
		this.aid = aid;
		this.oid = oid;
        this.isSystem = isSystem;
		if (isSystem)
			isSYSIBM = IBM_SYSTEM_SCHEMA_NAME.equals(name);
	}

	/**
	 * Gets the name of the schema 
	 *
	 * @return	The schema name
	 */
	public String	getSchemaName()
	{
		return name;
	}

	/**
	 * Gets the authorization id of the schema 
	 *
	 * @return	Authorization id
	 *		lives in.
	 */
	public String getAuthorizationId()
	{
		return aid;
	}

	/**
	 * Gets the oid of the schema 
	 *
	 * @return	An oid
	 */
	public UUID	getUUID()
	{
		return oid;
	}

	/**
	 * Sets the oid of the schema 
	 *
	 * @param oid	The object id
	 *
	 */
	public void	setUUID(UUID oid)
	{
		this.oid = oid;
	}

	//
	// Provider interface
	//

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
		// Is this OK?
	    return	getDependableFinder(StoredFormatIds.SCHEMA_DESCRIPTOR_FINDER_V01_ID);
	}

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return name;
	}

	/**
	 * Get the provider's UUID 
	 *
	 * @return String	The provider's UUID
	 */
	public UUID getObjectID()
	{
		return oid;
	}

	/**
	 * Get the provider's type.
	 *
	 * @return String		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.SCHEMA;
	}

	//
	// class interface
	//

	/**
	 * Prints the contents of the SchemaDescriptor
	 *
	 * @return The contents as a String
	 */
	public String toString()
	{
		return name;
	}

	//	Methods so that we can put SchemaDescriptors on hashed lists

	/**
	  *	Determine if two SchemaDescriptors are the same.
	  *
	  *	@param	otherObject	other schemadescriptor
	  *
	  *	@return	true if they are the same, false otherwise
	  */

	public boolean equals(Object otherObject)
	{
		if (!(otherObject instanceof SchemaDescriptor))
			return false;

		SchemaDescriptor other = (SchemaDescriptor) otherObject;

		if ((oid != null) && (other.oid != null))
			return oid.equals( other.oid);
		
		return name.equals(other.name);
	}

	/**
	 * Indicate whether this is a system schema or not
     *
     * Examples of system schema's include: 
     *      SYS, SYSIBM, SYSCAT, SYSFUN, SYSPROC, SYSSTAT, and SYSCS_DIAG 
	 *
	 * @return true/false
	 */
	public boolean isSystemSchema()
	{
		return(isSystem);
	}

	public boolean isSYSIBM()
	{
		return isSYSIBM;
	}

	/**
	  *	Get a hashcode for this SchemaDescriptor
	  *
	  *	@return	hashcode
	  */
	public int hashCode()
	{
		return	oid.hashCode();
	}
	
	/** @see TupleDescriptor#getDescriptorName */
	public String getDescriptorName() 
	{ 
		return name;
	}
	
	/** @see TupleDescriptor#getDescriptorType */
	public String getDescriptorType()
	{
		return "Schema";
	}
}
