/*

   Derby - Class org.apache.derby.catalog.types.BaseTypeIdImpl

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.catalog.types;

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.io.StreamStorable;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.info.JVMInfo;

import java.sql.Types;
import org.apache.derby.iapi.reference.JDBC20Translation; // needed for BLOB/CLOB types

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.EOFException;

/**
 * This class is the base class for all type ids that are written to the
 * system tables.
 */
public class BaseTypeIdImpl implements Formatable
{

    /********************************************************
    **
    **      This class implements Formatable. That means that it
    **      can write itself to and from a formatted stream. If
    **      you add more fields to this class, make sure that you
    **      also write/read them with the writeExternal()/readExternal()
    **      methods.
    **
    **      If, inbetween releases, you add more fields to this class,
    **      then you should bump the version number emitted by the 
    **      getTypeFormatId() method.
    **
    ********************************************************/

    protected String        SQLTypeName;
    protected int           JDBCTypeId;
    protected int           formatId;
    protected int           wrapperTypeFormatId;

    /**
     * niladic constructor. Needed for Formatable interface to work.
     *
     */

    public BaseTypeIdImpl() {}

    /**
     * 1 argument constructor. Needed for Formatable interface to work.
     *
     * @param formatId      Format id of specific type id.
     */
    public BaseTypeIdImpl(int formatId)
    {
        this.formatId = formatId;
        setTypeIdSpecificInstanceVariables();
    }

    /**
     * Constructor for an BaseTypeIdImpl
     *
     * @param SQLTypeName   The SQL name of the type
     */

    protected BaseTypeIdImpl(String SQLTypeName)
    {
        this.SQLTypeName = SQLTypeName;
    }

    /**
     * Returns the SQL name of the datatype. If it is a user-defined type,
     * it returns the full Java path name for the datatype, meaning the
     * dot-separated path including the package names.
     *
     * @return      A String containing the SQL name of this type.
     */
    public String   getSQLTypeName()
    {
        return SQLTypeName;
    }

    /**
     * Get the jdbc type id for this type.  JDBC type can be
     * found in java.sql.Types. 
     *
     * @return      a jdbc type, e.g. java.sql.Types.DECIMAL 
     *
     * @see Types
     */
    public int getJDBCTypeId()
    {
        return JDBCTypeId;
    }

    /** Does this type id represent a system built-in type? */
    public boolean systemBuiltIn()
    {
        return true;
    }

    /**
     * Converts this TypeId, given a data type descriptor 
     * (including length/precision), to a string. E.g.
     *
     *                      VARCHAR(30)
     *
     *
     *      For most data types, we just return the SQL type name.
     *
     *      @param  td      Data type descriptor that holds the 
     *                      length/precision etc. as necessary
     *
     *       @return        String version of datatype, suitable for running 
     *                      through the Parser.
     */
    public String   toParsableString(TypeDescriptor td)
    {
        String retval = getSQLTypeName();

        switch (formatId)
        {
          case StoredFormatIds.BIT_TYPE_ID_IMPL:
          case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
			  int rparen = retval.indexOf(')');
			  String lead = retval.substring(0, rparen);
			  retval = lead + td.getMaximumWidth() + retval.substring(rparen);
			  break;

          case StoredFormatIds.CHAR_TYPE_ID_IMPL:
          case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
          case StoredFormatIds.NATIONAL_CHAR_TYPE_ID_IMPL:
          case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID_IMPL:
          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
          case StoredFormatIds.NCLOB_TYPE_ID_IMPL:
                retval += "(" + td.getMaximumWidth() + ")";
                break;

          case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
                retval += "(" + td.getPrecision() + "," + td.getScale() + ")";
                break;
        }

        return retval;
    }

    /** Does this type id represent a user type? */
    public boolean userType()
    {
        return false;
    }

    /**
     * Format this BaseTypeIdImpl as a String
     *
     * @return      This BaseTypeIdImpl formatted as a String
     */

    public String   toString()
    {
        return MessageService.getTextMessage(SQLState.TI_SQL_TYPE_NAME) +
                ": " + SQLTypeName;
    }

    /**
     * we want equals to say if these are the same type id or not.
     */
    public boolean equals(Object that)
    {
        if (that instanceof BaseTypeIdImpl)
        {
            return this.SQLTypeName.equals(((BaseTypeIdImpl)that).getSQLTypeName());
        }
        else
        {
            return false;
        }
    }

    /**
      Hashcode which works with equals.
      */
    public int hashCode()
    {
        return this.SQLTypeName.hashCode();
    }

    /**
     * Get the format id for the wrapper type id that corresponds to
     * this type id.
     */
    public int wrapperTypeFormatId()
    {
        return wrapperTypeFormatId;
    }


    /**
     * Get the formatID which corresponds to this class.
     *
     * @return      the formatID of this class
     */
    public int getTypeFormatId()
    {
        return formatId;
    }

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     *
     * @exception IOException                       thrown on error
     * @exception ClassNotFoundException            thrown on error
     */
    public void readExternal( ObjectInput in )
             throws IOException, ClassNotFoundException
    {
        SQLTypeName = in.readUTF();
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     *
     * @exception IOException               thrown on error
     */
    public void writeExternal( ObjectOutput out )
             throws IOException
    {
        out.writeUTF( SQLTypeName );
    }

    private void setTypeIdSpecificInstanceVariables()
    {
        switch (formatId)
        {
          case StoredFormatIds.BOOLEAN_TYPE_ID_IMPL:
                SQLTypeName = TypeId.BOOLEAN_NAME;
                JDBCTypeId = JVMInfo.JAVA_SQL_TYPES_BOOLEAN;
                wrapperTypeFormatId = StoredFormatIds.BOOLEAN_TYPE_ID;
                break;

          case StoredFormatIds.INT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.INTEGER_NAME;
                JDBCTypeId = Types.INTEGER;
                wrapperTypeFormatId = StoredFormatIds.INT_TYPE_ID;
                break;

          case StoredFormatIds.SMALLINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.SMALLINT_NAME;
                JDBCTypeId = Types.SMALLINT;
                wrapperTypeFormatId = StoredFormatIds.SMALLINT_TYPE_ID;
                break;

          case StoredFormatIds.TINYINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TINYINT_NAME;
                JDBCTypeId = Types.TINYINT;
                wrapperTypeFormatId = StoredFormatIds.TINYINT_TYPE_ID;
                break;

          case StoredFormatIds.LONGINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGINT_NAME;
                JDBCTypeId = Types.BIGINT;
                wrapperTypeFormatId = StoredFormatIds.LONGINT_TYPE_ID;
                break;

          case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DECIMAL_NAME;
                JDBCTypeId = Types.DECIMAL;
                wrapperTypeFormatId = StoredFormatIds.DECIMAL_TYPE_ID;
                break;

          case StoredFormatIds.DOUBLE_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DOUBLE_NAME;
                JDBCTypeId = Types.DOUBLE;
                wrapperTypeFormatId = StoredFormatIds.DOUBLE_TYPE_ID;
                break;

          case StoredFormatIds.REAL_TYPE_ID_IMPL:
                SQLTypeName = TypeId.REAL_NAME;
                JDBCTypeId = Types.REAL;
                wrapperTypeFormatId = StoredFormatIds.REAL_TYPE_ID;
                break;
                
          case StoredFormatIds.REF_TYPE_ID_IMPL:
                SQLTypeName = TypeId.REF_NAME;
                JDBCTypeId = Types.OTHER;
                wrapperTypeFormatId = StoredFormatIds.REF_TYPE_ID;
                break;

          case StoredFormatIds.CHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.CHAR_NAME;
                JDBCTypeId = Types.CHAR;
                wrapperTypeFormatId = StoredFormatIds.CHAR_TYPE_ID;
                break;

          case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.VARCHAR_NAME;
                JDBCTypeId = Types.VARCHAR;
                wrapperTypeFormatId = StoredFormatIds.VARCHAR_TYPE_ID;
                break;

          case StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGVARCHAR_NAME;
                JDBCTypeId = Types.LONGVARCHAR;
                wrapperTypeFormatId = StoredFormatIds.LONGVARCHAR_TYPE_ID;
                break;

          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
                SQLTypeName = TypeId.CLOB_NAME;
                JDBCTypeId = JDBC20Translation.SQL_TYPES_CLOB;
                wrapperTypeFormatId = StoredFormatIds.CLOB_TYPE_ID;
                break;

          case StoredFormatIds.NATIONAL_CHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.NATIONAL_CHAR_NAME;
                JDBCTypeId = Types.CHAR;
                wrapperTypeFormatId = StoredFormatIds.NATIONAL_CHAR_TYPE_ID;
                break;

          case StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.NATIONAL_VARCHAR_NAME;
                JDBCTypeId = Types.VARCHAR;
                wrapperTypeFormatId = StoredFormatIds.NATIONAL_VARCHAR_TYPE_ID;
                break;

          case StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.NATIONAL_LONGVARCHAR_NAME;
                JDBCTypeId = Types.LONGVARCHAR;
                wrapperTypeFormatId = StoredFormatIds.NATIONAL_LONGVARCHAR_TYPE_ID;
                break;

         case StoredFormatIds.NCLOB_TYPE_ID_IMPL:
                SQLTypeName = TypeId.NCLOB_NAME;
                JDBCTypeId = JDBC20Translation.SQL_TYPES_CLOB;
                wrapperTypeFormatId = StoredFormatIds.NCLOB_TYPE_ID;
                break;

          case StoredFormatIds.BIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.BIT_NAME;
                JDBCTypeId = Types.BINARY;
                wrapperTypeFormatId = StoredFormatIds.BIT_TYPE_ID;
                break;

          case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.VARBIT_NAME;
                JDBCTypeId = Types.VARBINARY;
                wrapperTypeFormatId = StoredFormatIds.VARBIT_TYPE_ID;
                break;

          case StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGVARBIT_NAME;
                JDBCTypeId = Types.LONGVARBINARY;
                wrapperTypeFormatId = StoredFormatIds.LONGVARBIT_TYPE_ID;
                break;

          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
                SQLTypeName = TypeId.BLOB_NAME;
                JDBCTypeId = JDBC20Translation.SQL_TYPES_BLOB;
                wrapperTypeFormatId = StoredFormatIds.BLOB_TYPE_ID;
                break;

          case StoredFormatIds.DATE_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DATE_NAME;
                JDBCTypeId = Types.DATE;
                wrapperTypeFormatId = StoredFormatIds.DATE_TYPE_ID;
                break;

          case StoredFormatIds.TIME_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TIME_NAME;
                JDBCTypeId = Types.TIME;
                wrapperTypeFormatId = StoredFormatIds.TIME_TYPE_ID;
                break;

          case StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TIMESTAMP_NAME;
                JDBCTypeId = Types.TIMESTAMP;
                wrapperTypeFormatId = StoredFormatIds.TIMESTAMP_TYPE_ID;
                break;
          default:
                if (SanityManager.DEBUG)
                {
                        SanityManager.THROWASSERT("Unexpected formatId " + formatId);
                }
                break;
        }
    }
}
