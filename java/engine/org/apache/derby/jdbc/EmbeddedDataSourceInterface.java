/*

   Derby - Class org.apache.derby.jdbc.EmbeddedDataSourceInterface

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

/**
 * Methods that extend the API of
 * {@code javax.sql.DataSource} common for all Derby embedded data sources.
 * <p>
 * EmbeddedDataSourceInterface provides support for JDBC standard DataSource
 * attributes
 * <p>
 * The standard attributes provided by implementations are, cf. e.g. table
 * 9.1 in the JDBC 4.1 specification.
 * <ul>
 *   <li>databaseName</li>
 *   <li>dataSourceName</li>
 *   <li>description</li>
 *   <li>password</li>
 *   <li>user</li>
 * </ul>
 * The embedded Derby driver also supports these attributes:
 * <ul>
 *   <li>loginTimeout</li> @see javax.sql.CommonDataSource set/get
 *   <li>logWriter</li> @see javax.sql.CommonDataSource set/get
 *   <li>createDatabase</li>
 *   <li>connectionAttributes</li>
 *   <li>shutdownDatabase</li>
 *   <li>attributesAsPassword</li>
 * </ul>
 * <br>
 * See the specific Derby DataSource implementation for details on their
 * meaning.
 * <br>
 * See the JDBC specifications for more details.
 */
public interface EmbeddedDataSourceInterface extends javax.sql.DataSource {
    public void setDatabaseName(String databaseName);
   public String getDatabaseName();

    public void setDataSourceName(String dsn);
   public String getDataSourceName();

    public void setDescription(String desc);
   public String getDescription();

    public void setUser(String user);
   public String getUser();

    public void setPassword(String password);
   public String getPassword();

    public void setCreateDatabase(String create);
    public String getCreateDatabase();

   public void setConnectionAttributes(String prop);
   public String getConnectionAttributes();

   public void setShutdownDatabase(String shutdown);
   public String getShutdownDatabase();

   public void setAttributesAsPassword(boolean attributesAsPassword);
   public boolean getAttributesAsPassword();

}
