/*

   Derby - Class org.apache.derby.catalog.types.BaseTypeIdImpl

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

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.io.StreamStorable;

import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.JDBC40Translation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.info.JVMInfo;

import java.sql.Types;

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
    
    private int   formatId;

    String        SQLTypeName;
    /**
     * JDBC type - derived from the format identifier.
     */
    transient int           JDBCTypeId;

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

    BaseTypeIdImpl(String SQLTypeName)
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
          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
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
                ": " + getSQLTypeName();
    }

    /**
     * we want equals to say if these are the same type id or not.
     */
    public boolean equals(Object that)
    {
        if (that instanceof BaseTypeIdImpl)
        {
            return this.getSQLTypeName().equals(((BaseTypeIdImpl)that).getSQLTypeName());
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
        return this.getSQLTypeName().hashCode();
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
                break;

          case StoredFormatIds.INT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.INTEGER_NAME;
                JDBCTypeId = Types.INTEGER;
                 break;

          case StoredFormatIds.SMALLINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.SMALLINT_NAME;
                JDBCTypeId = Types.SMALLINT;
                break;

          case StoredFormatIds.TINYINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TINYINT_NAME;
                JDBCTypeId = Types.TINYINT;
                break;

          case StoredFormatIds.LONGINT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGINT_NAME;
                JDBCTypeId = Types.BIGINT;
                break;

          case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DECIMAL_NAME;
                JDBCTypeId = Types.DECIMAL;
                break;

          case StoredFormatIds.DOUBLE_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DOUBLE_NAME;
                JDBCTypeId = Types.DOUBLE;
                break;

          case StoredFormatIds.REAL_TYPE_ID_IMPL:
                SQLTypeName = TypeId.REAL_NAME;
                JDBCTypeId = Types.REAL;
                break;
                
          case StoredFormatIds.REF_TYPE_ID_IMPL:
                SQLTypeName = TypeId.REF_NAME;
                JDBCTypeId = Types.OTHER;
                break;

          case StoredFormatIds.CHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.CHAR_NAME;
                JDBCTypeId = Types.CHAR;
                break;

          case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.VARCHAR_NAME;
                JDBCTypeId = Types.VARCHAR;
                break;

          case StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGVARCHAR_NAME;
                JDBCTypeId = Types.LONGVARCHAR;
                break;

          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
                SQLTypeName = TypeId.CLOB_NAME;
                JDBCTypeId = Types.CLOB;
                break;

          case StoredFormatIds.BIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.BIT_NAME;
                JDBCTypeId = Types.BINARY;
                break;

          case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.VARBIT_NAME;
                JDBCTypeId = Types.VARBINARY;
               break;

          case StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL:
                SQLTypeName = TypeId.LONGVARBIT_NAME;
                JDBCTypeId = Types.LONGVARBINARY;
                break;

          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
                SQLTypeName = TypeId.BLOB_NAME;
                JDBCTypeId = Types.BLOB;
                break;

          case StoredFormatIds.DATE_TYPE_ID_IMPL:
                SQLTypeName = TypeId.DATE_NAME;
                JDBCTypeId = Types.DATE;
                break;

          case StoredFormatIds.TIME_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TIME_NAME;
                JDBCTypeId = Types.TIME;
                 break;

          case StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL:
                SQLTypeName = TypeId.TIMESTAMP_NAME;
                JDBCTypeId = Types.TIMESTAMP;
                break;

          case StoredFormatIds.XML_TYPE_ID_IMPL:
                SQLTypeName = TypeId.XML_NAME;
                JDBCTypeId = JDBC40Translation.SQLXML;
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
