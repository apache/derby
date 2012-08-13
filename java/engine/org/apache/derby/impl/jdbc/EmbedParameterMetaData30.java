/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedParameterMetaData30

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

package org.apache.derby.impl.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * This class implements the ParameterMetaData interface from JDBC 3.0 and 4.0.
 * It provides the parameter meta data for callable & prepared statements
 * But note that the bulk of it resides in its parent class.  The reason is
 * we want to provide the functionality to the JDKs before JDBC3.0.
 *
  <P><B>Supports</B>
   <UL>
   <LI> JDBC 3.0 - java.sql.ParameterMetaData introduced in JDBC3
   <LI> JDBC 4.0 - extra methods from java.sql.Wrapper introduced in JDBC 4.0
   </UL>

 * @see java.sql.ParameterMetaData
 *
 */
class EmbedParameterMetaData30 extends org.apache.derby.impl.jdbc.EmbedParameterSetMetaData
    implements ParameterMetaData {

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
    EmbedParameterMetaData30(ParameterValueSet pvs, DataTypeDescriptor[] types)  {
		super(pvs, types);
    }

    /**
     * Returns false unless {@code iface}</code> is implemented.
     *
     * @param iface a class defining an interface
     * @return true if this implements the interface or directly or indirectly
     * wraps an object that does
     * @throws SQLException if an error occurs while determining
     * whether this is a wrapper for an object with the given interface.
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /**
     * Returns {@code this} if this class implements the specified interface.
     *
     * @param iface a class defining an interface
     * @return an object that implements the interface
     * @throws SQLException if no object is found that implements the
     * interface
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // Derby does not implement non-standard methods on JDBC objects,
        // hence return this if this class implements the interface, or
        // throw an SQLException.
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP,
                    iface);
        }
    }
}
