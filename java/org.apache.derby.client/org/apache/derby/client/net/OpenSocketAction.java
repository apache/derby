/*

   Derby - Class org.apache.derby.client.net.OpenSocketAction

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

package org.apache.derby.client.net;

import java.io.IOException;

import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivilegedExceptionAction;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.derby.client.BasicClientDataSource;
import org.apache.derby.shared.common.drda.NaiveTrustManager;

//IC see: https://issues.apache.org/jira/browse/DERBY-6125
class OpenSocketAction implements PrivilegedExceptionAction<Socket> {
    private String server_;
    private int port_;
    private int clientSSLMode_;

    OpenSocketAction(String server, int port, int clientSSLMode) {
        server_ = server;
        port_ = port;
        clientSSLMode_ = clientSSLMode;
    }

    @Override
    public Socket run()
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        throws UnknownHostException,
               IOException,
               NoSuchAlgorithmException,
               KeyManagementException,
               NoSuchProviderException,
               KeyStoreException,
               UnrecoverableKeyException,
               CertificateException
    {
        
        SocketFactory sf;
        switch (clientSSLMode_) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        case BasicClientDataSource.SSL_BASIC:
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
            Properties sslProperties = getSSLProperties();
            sf = NaiveTrustManager.getSocketFactory(sslProperties);
            break;
        case BasicClientDataSource.
                SSL_PEER_AUTHENTICATION:
            sf = (SocketFactory)SSLSocketFactory.getDefault();
            break;
        case BasicClientDataSource.SSL_OFF:
            sf = SocketFactory.getDefault();
            break;
        default: 
            // Assumes cleartext for undefined values
            sf = SocketFactory.getDefault();
            break;
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (clientSSLMode_ == BasicClientDataSource.SSL_BASIC ||
            clientSSLMode_ == BasicClientDataSource.SSL_PEER_AUTHENTICATION){
        	//DERBY-6764(analyze impact of poodle security alert on Derby 
        	// client - server ssl support)
        	//If SSLv3 and/or SSLv2Hello is one of the enabled protocols,  
        	// then we want to remove it from the list of enabled protocols  
        	// because of poodle security breach
        	SSLSocket sSocket = (SSLSocket)sf.createSocket(server_, port_);
        	String[] enabledProtocols = sSocket.getEnabledProtocols();

            //If SSLv3 and/or SSLv2Hello is one of the enabled protocols, 
            // then remove it from the list of enabled protocols because of 
            // its security breach.
            String[] supportedProtocols = new String[enabledProtocols.length];
            int supportedProtocolsCount  = 0;
            for ( int i = 0; i < enabledProtocols.length; i++ )
            {
                if (!(enabledProtocols[i].toUpperCase().contains("SSLV3") ||
                    enabledProtocols[i].toUpperCase().contains("SSLV2HELLO"))) {
                	supportedProtocols[supportedProtocolsCount] = 
                			enabledProtocols[i];
                	supportedProtocolsCount++;
                }
            }
            if(supportedProtocolsCount < enabledProtocols.length) {
            	String[] newEnabledProtocolsList = null;
            	//We found that SSLv3 and or SSLv2Hello is one of the enabled 
            	// protocols for this jvm. Following code will remove it from 
            	// enabled list.
            	newEnabledProtocolsList = 
            			new String[supportedProtocolsCount];
            	System.arraycopy(supportedProtocols, 0, 
            			newEnabledProtocolsList, 0, 
            			supportedProtocolsCount);
            	sSocket.setEnabledProtocols(newEnabledProtocolsList);
            }
            return sSocket;
        } else
            return sf.createSocket(server_, port_);
    }

    /**
     * Retrieve the settings of the SSL properties
     */
    private Properties getSSLProperties()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        Properties retval = new Properties();
        
        String keyStoreProp = System.getProperty(NaiveTrustManager.SSL_KEYSTORE);
        if (keyStoreProp != null)
        { retval.setProperty(NaiveTrustManager.SSL_KEYSTORE, keyStoreProp); }

        String keyStorePasswordProp = System.getProperty(NaiveTrustManager.SSL_KEYSTORE_PASSWORD);
        if (keyStoreProp != null)
        { retval.setProperty(NaiveTrustManager.SSL_KEYSTORE_PASSWORD, keyStorePasswordProp); }

        return retval;
    }

}
