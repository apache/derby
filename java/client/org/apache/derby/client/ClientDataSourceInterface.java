/*

   Derby - Class org.apache.derby.client.ClientDataSourceInterface

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

package org.apache.derby.client;

import javax.sql.DataSource;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.net.NetConfiguration;

/**
 * Specifies Derby extensions to the {@code java.sqlx.DataSource}
 * API common to all Derby client driver data sources.
 */
public interface ClientDataSourceInterface extends DataSource {

    public void setPassword(String password);
    public String getPassword();

    public void setDatabaseName(String databaseName);
    public String getDatabaseName();

    public void setDataSourceName(String dataSourceName);
    public String getDataSourceName();

    public void setDescription(String description);
    public String getDescription();

    public final static int propertyDefault_portNumber = 1527;
    public void setPortNumber(int portNumber);
    public int getPortNumber();

    public final static String propertyDefault_serverName = "localhost";
    public void setServerName(String serverName);
    public String getServerName();

    public final static String propertyDefault_user = "APP";

    public void setUser(String user);
    public String getUser();

    public final static boolean propertyDefault_retrieveMessageText = true;
    public void setRetrieveMessageText(boolean retrieveMessageText);
    public boolean getRetrieveMessageText();

    /**
     * The source security mechanism to use when connecting to a client data
     * source.
     * <p/>
     * Security mechanism options are:
     * <ul>
     *   <li> USER_ONLY_SECURITY
     *   <li> CLEAR_TEXT_PASSWORD_SECURITY
     *   <li> ENCRYPTED_PASSWORD_SECURITY
     *   <li> ENCRYPTED_USER_AND_PASSWORD_SECURITY - both password and
     *        user are encrypted
     *   <li> STRONG_PASSWORD_SUBSTITUTE_SECURITY
     * </ul> The default security mechanism is USER_ONLY SECURITY
     * <p/>
     * If the application specifies a security mechanism then it will be the
     * only one attempted. If the specified security mechanism is not
     * supported by the conversation then an exception will be thrown and
     * there will be no additional retries.
     * <p/>
     * Both user and password need to be set for all security mechanism except
     * USER_ONLY_SECURITY.
     */
    public final static short USER_ONLY_SECURITY =
        (short)NetConfiguration.SECMEC_USRIDONL;

    public final static short CLEAR_TEXT_PASSWORD_SECURITY =
        (short)NetConfiguration.SECMEC_USRIDPWD;

    public final static short ENCRYPTED_PASSWORD_SECURITY =
        (short)NetConfiguration.SECMEC_USRENCPWD;

    public final static short ENCRYPTED_USER_AND_PASSWORD_SECURITY =
        (short)NetConfiguration.SECMEC_EUSRIDPWD;

    public final static short STRONG_PASSWORD_SUBSTITUTE_SECURITY =
        (short)NetConfiguration.SECMEC_USRSSBPWD;

    /**
     * Default security mechanism is USER_ONLY_SECURITY.
     */
    public final static short propertyDefault_securityMechanism =
        (short)NetConfiguration.SECMEC_USRIDONL;

    public void setSecurityMechanism(short securityMechanism);
    public short getSecurityMechanism();
    public short getSecurityMechanism(String password);

    public void setSsl(String mode) throws SqlException;
    public String getSsl();

    public void setCreateDatabase(String create);
    public String getCreateDatabase();

    public void setShutdownDatabase(String shutdown);
    public String getShutdownDatabase();

    public void setConnectionAttributes(String prop);
    public String getConnectionAttributes();

    public void setTraceLevel(int traceLevel);
    public int getTraceLevel();

    public void setTraceFile(String traceFile);
    public String getTraceFile();

    public void setTraceDirectory(String traceDirectory);
    public String getTraceDirectory();

    public final static boolean propertyDefault_traceFileAppend = false;

    public void setTraceFileAppend(boolean traceFileAppend);
    public boolean getTraceFileAppend();


}
