/*

   Derby - Class org.apache.derby.iapi.types.SQLNClob

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.i18n.LocaleFinder;

import java.util.Locale;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * SQLNClob satisfies the DataValueDescriptor interfaces (i.e., OrderableDataType). It implements a String
 * holder, e.g. for storing a column value; it can be specified
 * when constructed to not allow nulls. Nullability cannot be changed
 * after construction.
 * <p>
 *** ----- TODO: fix for NCLOB
 * Because OrderableDataType is a subclass of DataType,
 * SQLNationalLongvarchar can play a role in either a DataType/ValueRow
 * or a OrderableDataType/KeyRow, interchangeably.
 *** ----- TODO: fix for NCLOB
 * SQLNationalLongvarchar is mostly the same as SQLLongvarchar, so it is implemented as a
 * subclass of SQLLongvarchar.  Only those methods with different behavior are
 * implemented here.
 */
public class SQLNClob
        extends SQLNationalVarchar
{
        /*
         * DataValueDescriptor interface.
         *
         * These are actually all implemented in the super-class, but we need
         * to duplicate some of them here so they can be called by byte-code
         * generation, which needs to know the class the method appears in.
         */

        public String getTypeName()
        {
                return TypeId.NCLOB_NAME;
        }

        /*
         * DataValueDescriptor interface
         */

        /** @see DataValueDescriptor#getClone */
        public DataValueDescriptor getClone()
        {
                try
                {
                        /* NOTE: We pass instance variables for locale info 
                         * because we only call methods when we know that we
                         * will need locale info.
                         */
                        return new SQLNClob(getString(), getLocaleFinder());
                }
                catch (StandardException se)
                {
                        if (SanityManager.DEBUG)
                                SanityManager.THROWASSERT("Unexpected exception " + se);
                        return null;
                }
        }

        /**
         * @see DataValueDescriptor#getNewNull
         *
         */
        public DataValueDescriptor getNewNull()
        {
                /* NOTE: We pass instance variables for locale info 
                 * because we only call methods when we know that we
                 * will need locale info.
                 */
                SQLNClob result = new SQLNClob();
                result.setLocaleFinder(getLocaleFinder());
                return result;
        }

        /*
         * Storable interface, implies Externalizable, TypedFormat
         */

        /**
                Return my format identifier.

                @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
        */
        public int getTypeFormatId() {
                return StoredFormatIds.SQL_NCLOB_ID;
        }

        /*
         * constructors
         */

        public SQLNClob()
        {
        }

        public SQLNClob(String val, LocaleFinder localeFinder)
        {
                super(val, localeFinder);
        }

        /**
         * @see DataValueDescriptor#getDate
         * @exception StandardException thrown on failure to convert
         */
        public Date     getDate( Calendar cal) throws StandardException
        {
                return nationalGetDate(cal);
        }

        /**
         * @see DataValueDescriptor#getTime
         * @exception StandardException thrown on failure to convert
         */
        public Time getTime( Calendar cal) throws StandardException
        {
                return nationalGetTime(cal);
        }

        /**
         * @see DataValueDescriptor#getTimestamp
         * @exception StandardException thrown on failure to convert
         */
        public Timestamp getTimestamp( Calendar cal) throws StandardException
        {
                return nationalGetTimestamp(cal);
        }

        /*
         * DataValueDescriptor interface
         */

        /* @see DataValueDescriptor#typePrecedence */
        public int typePrecedence()
        {
                return TypeId.NCLOB_PRECEDENCE;
        }

        /** 
     ****  ---- TODO: Disable?
         * Compare two SQLChars.  This method will be overriden in the
         * National char wrappers so that the appropriate comparison
         * is done.
         *
         * @exception StandardException         Thrown on error
         */
         protected int stringCompare(SQLChar char1, SQLChar char2)
                 throws StandardException
         {
                 return char1.stringCollatorCompare(char2);              
         }

        /**
         * Get a SQLVarchar for a built-in string function.  
         * (Could be either a SQLVarchar or SQLNationalVarchar.)
         *
         * @return a SQLVarchar or SQLNationalVarchar.
         */
        protected StringDataValue getNewVarchar()
        {
                return new SQLNationalVarchar();
        }

        /** 
         * Return whether or not this is a national character datatype.
         */
        protected boolean isNationalString()
        {
                return true;
        }

        /**
         * @see DataValueDescriptor#setValue
         *
         * @exception StandardException         Thrown on error
         */
        public void setValue(Date theValue, Calendar cal) throws StandardException
        {
                setValue(getDateFormat(cal).format(theValue));
        }

        /**
         *  @see DataValueDescriptor#setValue
         *
         * @exception StandardException         Thrown on error
         */
        public void setValue(Time theValue, Calendar cal) throws StandardException
        {
                setValue(getTimeFormat(cal).format(theValue));
        }

        /**
         *  @see DataValueDescriptor#setValue
         *
         * @exception StandardException         Thrown on error
         */
        public void setValue(Timestamp theValue, Calendar cal) throws StandardException
        {
                setValue(getTimestampFormat(cal).format(theValue));
        }
        protected void setFrom(DataValueDescriptor theValue) throws StandardException {

                setValue(((DataType) theValue).getNationalString(getLocaleFinder()));
        }

        /** @see java.lang.Object#hashCode */
        public int hashCode()
        {
                return nationalHashCode();
        }
}
