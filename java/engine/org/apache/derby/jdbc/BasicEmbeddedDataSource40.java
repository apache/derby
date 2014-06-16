/*

   Derby - Class org.apache.derby.jdbc.BasicEmbeddedDataSource40

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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import org.apache.derby.impl.jdbc.Util;

/**
 *
 * This data source is suitable for an application using embedded Derby,
 * running on Java 8 Compact Profile 2 or higher.
 * <p/>
 * BasicEmbeddedDataSource40 is similar to EmbeddedDataSource40, but does
 * not support JNDI naming, i.e. it does not implement
 * {@code javax.naming.Referenceable}.
 *
 * @see EmbeddedDataSource40
 */
public class BasicEmbeddedDataSource40 extends EmbeddedBaseDataSource
    implements javax.sql.DataSource {

    private static final long serialVersionUID = -4945135214995641182L;

    public BasicEmbeddedDataSource40() {}

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        super.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return super.getLoginTimeout();
    }

    @Override
    public void setLogWriter(PrintWriter logWriter)
            throws SQLException {
        super.setLogWriter(logWriter);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return super.getLogWriter();
    }

    @Override
    public final void setPassword(String password) {
        super.setPassword(password);
    }

    @Override
    public final String getPassword() {
        return super.getPassword();
    }

    @Override
    public void setDatabaseName(String databaseName) {
        super.setDatabaseName(databaseName);
    }

    @Override
    public String getDatabaseName() {
        return super.getDatabaseName();
    }

    @Override
    public void setDataSourceName(String dataSourceName) {
        super.setDataSourceName(dataSourceName);
    }

    @Override
    public String getDataSourceName() {
        return super.getDataSourceName();
    }

    @Override
    public void setDescription(String description) {
        super.setDescription(description);
    }

    @Override
    public String getDescription() {
        return super.getDescription();
    }

    @Override
    public void setUser(String user) {
        super.setUser(user);
    }

    @Override
    public String getUser() {
        return super.getUser();
    }

    @Override
    public final void setCreateDatabase(String create) {
        super.setCreateDatabase(create);
    }

    @Override
    public final String getCreateDatabase() {
        return super.getCreateDatabase();
    }

    @Override
    public final void setShutdownDatabase(String shutdown) {
        super.setShutdownDatabase(shutdown);
    }

    @Override
    public final String getShutdownDatabase() {
        return super.getShutdownDatabase();
    }

    @Override
    public final void setConnectionAttributes(String prop) {
        super.setConnectionAttributes(prop);
    }

    @Override
    public final String getConnectionAttributes() {
        return super.getConnectionAttributes();
    }


    @Override
    public Connection getConnection() throws SQLException {
        return super.getConnection();
    }

    @Override
    public Connection getConnection(String user, String password)
            throws SQLException {
        return super.getConnection(user, password);
    }

    @Override
    public final Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return super.getParentLogger();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return super.unwrap(iface);
    }

    @Override
    public final void setAttributesAsPassword(boolean attributesAsPassword) {
        super.setAttributesAsPassword(attributesAsPassword);
    }

    @Override
    public final boolean getAttributesAsPassword() {
        return super.getAttributesAsPassword();
    }
}
