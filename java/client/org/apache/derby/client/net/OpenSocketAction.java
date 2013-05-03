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
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.apache.derby.jdbc.ClientBaseDataSourceRoot;

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
        case ClientBaseDataSourceRoot.SSL_BASIC:
            sf = NaiveTrustManager.getSocketFactory();
            break;
        case ClientBaseDataSourceRoot.
                SSL_PEER_AUTHENTICATION:
            sf = (SocketFactory)SSLSocketFactory.getDefault();
            break;
        case ClientBaseDataSourceRoot.SSL_OFF:
            sf = SocketFactory.getDefault();
            break;
        default: 
            // Assumes cleartext for undefined values
            sf = SocketFactory.getDefault();
            break;
        }
        return sf.createSocket(server_, port_);
    }

}
