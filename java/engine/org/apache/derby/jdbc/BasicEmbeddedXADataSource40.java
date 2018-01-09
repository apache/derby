/*

   Derby - Class org.apache.derby.jdbc.BasicEmbeddedXADataSource40

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

import java.sql.SQLException;
import javax.sql.XAConnection;
import org.apache.derby.iapi.jdbc.ResourceAdapter;

/**
 *
 * This data source is suitable for an application using embedded Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicEmbeddedXADataSource40 is similar to
 * EmbeddedXADataSource40, except that it does not support JNDI
 * naming, i.e. it does not implement {@code javax.naming.Referenceable}.
 *
 * @see EmbeddedXADataSource40
 */
public class BasicEmbeddedXADataSource40
    extends BasicEmbeddedDataSource40
    implements
        EmbeddedXADataSourceInterface,
        javax.sql.XADataSource /* compile time check of 41 extensions */
{

    private static final long serialVersionUID = -5715798975598379739L;

    /**
     * link to the database
     */
    private transient ResourceAdapter ra;

    /**
     * Constructs a basic embedded XA data source. See the class Javadoc.
     */
    public BasicEmbeddedXADataSource40() {
        super();
    }

    /*
     * Implementation of XADataSource interface methods
     */

    @Override
    public final XAConnection getXAConnection() throws SQLException     {

        if (ra == null || !ra.isActive()) {
            ra = setupResourceAdapter(this, ra, null, null, false);
        }

        return createXAConnection (ra, getUser(), getPassword(), false);
    }

    @Override
    public final XAConnection getXAConnection(String user, String password)
         throws SQLException {

        if (ra == null || !ra.isActive()) {
            ra = setupResourceAdapter(this, ra, user, password, true);
        }

        return createXAConnection (ra, user, password, true);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Also clears {@link #ra}.
     */
    @Override
    protected void update() {
        ra = null;
        super.update();
    }


    /**
     * Instantiate and return an EmbedXAConnection from this instance
     * of EmbeddedXADataSource. Minion method.
     * @param ra The resource adapter for this database
     * @param user The user name
     * @param password The password
     * @param requestPassword @{@code false} if original call is from a
     *        no-argument constructor, otherwise {@code true}
     * @return An XA connection to the database
     */
    private XAConnection createXAConnection(
            ResourceAdapter ra,
            String user,
            String password,
            boolean requestPassword) throws SQLException {

        // See comment for EmbeddedXADataSource#createXAConnection
        findDriver();
        return new EmbedXAConnection(this, ra, user, password, requestPassword);
    }


    /**
     * @return The ResourceAdapter instance for the underlying database
     */
    @Override
    public ResourceAdapter getResourceAdapter() {
        return ra;
    }
}
