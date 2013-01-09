/*

   Derby - Class org.apache.derby.jdbc.ClientBaseDataSource

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

package org.apache.derby.jdbc;

import java.util.Enumeration;
import java.util.Properties;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import org.apache.derby.client.ClientDataSourceFactory;

/**
 * Base class for client-side DataSource implementations.
 */
public abstract class ClientBaseDataSource extends ClientBaseDataSourceRoot
    implements Referenceable {

    private static final long serialVersionUID = -7660172643035173692L;

    //------------------------ interface methods -----------------------------

    public Reference getReference() throws NamingException {

        // This method creates a new Reference object to represent this data
        // source.  The class name of the data source object is saved in the
        // Reference, so that an object factory will know that it should
        // create an instance of that class when a lookup operation is
        // performed. The class name of the object factory,
        // org.apache.derby.client.ClientBaseDataSourceFactory, is also stored
        // in the reference.  This is not required by JNDI, but is recommend
        // in practice.  JNDI will always use the object factory class
        // specified in the reference when reconstructing an object, if a
        // class name has been specified.
        //
        // See the JNDI SPI documentation for further details on this topic,
        // and for a complete description of the Reference and StringRefAddr
        // classes.
        //
        // This ClientBaseDataSource class provides several standard JDBC
        // properties.  The names and values of the data source properties are
        // also stored in the reference using the StringRefAddr class.  This
        // is all the information needed to reconstruct a ClientBaseDataSource
        // object.

        Reference ref = new Reference(this.getClass().getName(),
                ClientDataSourceFactory.class.getName(), null);

        addBeanProperties(ref);
        return ref;
    }

    /**
     * Add Java Bean properties to the reference using
     * StringRefAddr for each property. List of bean properties
     * is defined from the public getXXX() methods on this object
     * that take no arguments and return short, int, boolean or String.
     * The StringRefAddr has a key of the Java bean property name,
     * converted from the method name. E.g. traceDirectory for
     * traceDirectory.
     *
      */
    private void addBeanProperties(Reference ref) {

        Properties p = getProperties(this);
        Enumeration e = p.propertyNames();

        while (e.hasMoreElements()) {
            String propName = (String)e.nextElement();
            Object value = p.getProperty(propName);
            if (value != null) {
                ref.add(new StringRefAddr(propName, value.toString()));
            }
        }
    }
}
