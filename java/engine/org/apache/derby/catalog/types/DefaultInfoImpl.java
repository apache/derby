/*

   Derby - Class org.apache.derby.catalog.types.DefaultInfoImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.catalog.types;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.catalog.DefaultInfo;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class DefaultInfoImpl implements DefaultInfo, Formatable
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

	private DataValueDescriptor	defaultValue;
	private String				defaultText;
	private int                     type;
    private String[]                   referencedColumnNames;
    private String                  originalCurrentSchema;

	final private static int BITS_MASK_IS_DEFAULTVALUE_AUTOINC = 0x1 << 0;
	final private static int BITS_MASK_IS_GENERATED_COLUMN = 0x2;

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	DefaultInfoImpl() {}

	/**
	 * Constructor for use with numeric types
	 *
	 * @param defaultText	The text of the default.
	 */
	public DefaultInfoImpl(boolean isDefaultValueAutoinc,
		String defaultText,
		DataValueDescriptor defaultValue)
	{
		this.type = calcType(isDefaultValueAutoinc);
		this.defaultText = defaultText;
		this.defaultValue = defaultValue;
	}

	/**
	 * Constructor for use with generated columns
     *
     * @param   defaultText Text of generation clause.
     * @param   referencedColumnsNames  names of other columns in the base row which are mentioned in the generation clause.
     * @param   originalCurrentSchema   Schema in effect when the generate column was added to the table.
	 */
	public DefaultInfoImpl
        (
         String defaultText,
         String[]    referencedColumnNames,
         String originalCurrentSchema
         )
	{
        if ( referencedColumnNames == null ) { referencedColumnNames = new String[0]; }
        
		this.type = BITS_MASK_IS_GENERATED_COLUMN;
		this.defaultText = defaultText;
		this.referencedColumnNames = referencedColumnNames;
        this.originalCurrentSchema = originalCurrentSchema;
	}

	/**
	 * @see DefaultInfo#getDefaultText
	 */
	public String getDefaultText()
	{
		return defaultText;
	}

	/**
	 * @see DefaultInfo#getReferencedColumnNames
	 */
	public String[] getReferencedColumnNames()
	{
		return referencedColumnNames;
	}

	/**
	 * @see DefaultInfo#getOriginalCurrentSchema
	 */
	public String   getOriginalCurrentSchema()
	{
		return originalCurrentSchema;
	}

	public String	toString()
	{
		if(isDefaultValueAutoinc()){
			return "GENERATED_BY_DEFAULT";
		}
        else if ( isGeneratedColumn() )
        {
            return "GENERATED ALWAYS AS ( " + defaultText + " )";
        }
        else { return defaultText; }
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
		defaultText = (String) in.readObject();
		defaultValue = (DataValueDescriptor) in.readObject();
		type = in.readInt();

        if ( isGeneratedColumn() )
        {
            int count = in.readInt();
            referencedColumnNames = new String[ count ];
            for ( int i = 0; i < count; i++ ) { referencedColumnNames[ i ] = (String) in.readObject(); }
            originalCurrentSchema = (String) in.readObject();
        }
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
		out.writeObject( defaultText );
		out.writeObject( defaultValue );
		out.writeInt(type);
        
        if ( isGeneratedColumn() )
        {
            int count = referencedColumnNames.length;
            out.writeInt( count );
            for ( int i = 0; i < count; i++ ) { out.writeObject( referencedColumnNames[ i ] ); }
            out.writeObject( originalCurrentSchema );
        }
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.DEFAULT_INFO_IMPL_V01_ID; }

	/**
	 * Get the default value.
	 * (NOTE: This returns null if 
	 * the default is not a constant.)
	 *
	 * @return The default value.
	 */
	public DataValueDescriptor getDefaultValue()
	{
		return defaultValue;
	}

	/**
	 * Set the default value.
	 *
	 * @param defaultValue The default value.
	 */
	public void setDefaultValue(DataValueDescriptor defaultValue)
	{
		this.defaultValue = defaultValue;
	}
	
	/**
	 * @see DefaultInfo#isDefaultValueAutoinc
	 */
	public boolean isDefaultValueAutoinc(){
		return (type & BITS_MASK_IS_DEFAULTVALUE_AUTOINC ) != 0;
	}
	
	/**
	 * @see DefaultInfo#isGeneratedColumn
	 */
	public boolean isGeneratedColumn(){
		return (type & BITS_MASK_IS_GENERATED_COLUMN ) != 0;
	}
	
	/**
	 * This function returns stored value for flags and so on.
	 */
	private static int calcType(boolean isDefaultValueAutoinc){

		int value = 0;

		if(isDefaultValueAutoinc){
			value |= BITS_MASK_IS_DEFAULTVALUE_AUTOINC;
		}

		return value;
	}
	
}
