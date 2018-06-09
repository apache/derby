/*

   Derby - Class org.apache.derby.impl.services.jce.JCECipherFactoryBuilder

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

package org.apache.derby.impl.services.jce;

import org.apache.derby.iapi.services.crypto.CipherFactory;
import org.apache.derby.iapi.services.crypto.CipherFactoryBuilder;
import org.apache.derby.shared.common.error.StandardException;
import java.util.Properties;


/**
 * Cipher Factory instance builder. New instances of the cipher 
 * factory are created based on the on the user specified 
 * encryption properties.
 */

public class JCECipherFactoryBuilder implements CipherFactoryBuilder
{

    
	public JCECipherFactoryBuilder() {
	}


    /**
     * Create an instance of the cipher factory.
     *
     * @param create    true, if the database is getting configured 
     *                  for encryption.
     * @param props	    encryption properties/attributes to use
     *                  for creating the cipher factory.
     * @param newAttrs  true, if cipher factory has to be created using 
     *                  should using the new attributes specified by the user.  
     */
    public CipherFactory createCipherFactory(boolean create, 
                                             Properties props, 
                                             boolean newAttrs) 
        throws StandardException

    {
        return new JCECipherFactory(create, props, newAttrs);
    }
}
