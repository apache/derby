/*

   Derby - Class org.apache.derby.client.ClientDataSourceFactory

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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

package org.apache.derby.client;

import org.apache.derby.jdbc.ClientConnectionPoolDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.ClientXADataSource;

/**
 * The data source factory currrently for ClientDataSource only. This factory will support XA and pooling-enabled data
 * sources in the future.
 * <p/>
 * This factory reconstructs a DERBY simple data source object when it is retrieved from JNDI. References are needed
 * since many naming services don't have the ability to store Java objects in their serialized form. When a data source
 * object is bound in this type of naming service the Reference for that object is actually stored by the JNDI
 * implementation, not the data source object itself.
 * <p/>
 * A JNDI administrator is responsible for making sure that both the object factory and data source implementation
 * classes provided by a JDBC driver vendor are accessible to the JNDI service provider at runtime.
 * <p/>
 * An object factory implements the javax.naming.spi.ObjectFactory interface. This interface contains a single method,
 * getObjectInstance, which is called by a JNDI service provider to reconstruct an object when that object is retrieved
 * from JNDI. A JDBC driver vendor should provide an object factory as part of their JDBC 2.0 product.
 *
 * @see ClientDataSource
 */
public class ClientDataSourceFactory implements javax.naming.spi.ObjectFactory {

    public ClientDataSourceFactory() {
    }

    /**
     * Reconstructs a ClientDataSource object from a JNDI data source reference.
     * <p/>
     * The getObjectInstance() method is passed a reference that corresponds to the object being retrieved as its first
     * parameter. The other parameters are optional in the case of JDBC data source objects. The object factory should
     * use the information contained in the reference to reconstruct the data source. If for some reason, a data source
     * object cannot be reconstructed from the reference, a value of null may be returned. This allows other object
     * factories that may be registered in JNDI to be tried. If an exception is thrown then no other object factories
     * are tried.
     *
     * @param refObj      The possibly null object containing location or reference information that can be used in
     *                    creating an object.
     * @param name        The name of this object relative to nameContext, or null if no name is specified.
     * @param nameContext Context relative to which the name parameter is specified, or null if name is relative to the
     *                    default initial context.
     * @param environment Possibly null environment that is used in creating the object.
     *
     * @return object created; null if an object cannot be created
     */
    public Object getObjectInstance(Object refObj,
                                    javax.naming.Name name,
                                    javax.naming.Context nameContext,
                                    java.util.Hashtable environment) throws java.lang.Exception {
        javax.naming.Reference ref = (javax.naming.Reference) refObj;

        // Create the proper data source object shell.
        ClientDataSource ds = null;
        if (ref.getClassName().equals(ClientDataSource.className__)) {
            ds = new ClientDataSource();
        } else if (ref.getClassName().equals(ClientXADataSource.className__)) {
            ds = new ClientXADataSource();
        } else if (ref.getClassName().equals(ClientConnectionPoolDataSource.className__)) {
            ds = new ClientConnectionPoolDataSource();
        } else {
            return null;
        }

        // Fill in the data source object shell with values from the jndi reference.
        ds.hydrateFromReference(ref);

        return ds;
    }
}
