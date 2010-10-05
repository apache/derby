/*

   Derby - Class org.apache.derby.client.net.Utf8CcsidManager

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

import java.io.UnsupportedEncodingException;

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.shared.common.reference.SQLState;

public class Utf8CcsidManager extends CcsidManager {

    public Utf8CcsidManager() {
        super((byte) ' ', // 0x40 is the ebcdic space character
                (byte) '.',
                new byte[]{//02132002jev begin
                    //     '0',       '1',       '2',       '3',      '4',
                    (byte) 0xf0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4,
                    //     '5',       '6',       '7',       '8',      '9',
                    (byte) 0xf5, (byte) 0xf6, (byte) 0xf7, (byte) 0xf8, (byte) 0xf9,
                    //     'A',       'B',       'C',       'D',      'E',
                    (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
                    //      'F'
                    (byte) 0xc6},
                new byte[]{
                    //     'G',       'H',       'I',       'J',      'K',
                    (byte) 0xc7, (byte) 0xc8, (byte) 0xc9, (byte) 0xd1, (byte) 0xd2,
                    //     'L',       'M',       'N',       '0',      'P',
                    (byte) 0xd3, (byte) 0xd4, (byte) 0xd5, (byte) 0xd6, (byte) 0xd7,
                    //     'A',       'B',       'C',       'D',      'E',
                    (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
                    //      'F'
                    (byte) 0xc6}                     //02132002jev end
        );
    }
    
    public byte[] convertFromJavaString(String sourceString, Agent agent)
    throws SqlException {
        byte[] bytes = new byte[getByteLength(sourceString)];
        convertFromJavaString(sourceString, bytes, 0, agent);
        return bytes;
    }
    
    public String convertToJavaString(byte[] sourceBytes) {
        return convertToJavaString(sourceBytes, 0, sourceBytes.length);
    }

    /**
     * Offset and numToConvert are given in terms of bytes! Not characters!
     */
    public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
        try {
            return new String(sourceBytes, offset, numToConvert, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // We don't have an agent in this method
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not convert byte[] to Java String using UTF-8 encoding with offset",e);
            }
        }
        return null;
    }

    public int convertFromJavaString(String sourceString, byte[] buffer,
            int offset, Agent agent) throws SqlException {
        try {
            byte[] strBytes = sourceString.getBytes("UTF-8"); 
            
            for(int i=0; i<strBytes.length; i++) {
                buffer[offset++] = strBytes[i];
            }
        } catch (UnsupportedEncodingException e) {
            throw new SqlException(agent.logWriter_, 
                    new ClientMessageId(SQLState.CANT_CONVERT_UNICODE_TO_UTF8));
        }
        return offset;
    }

    int maxBytesPerChar() {
        return 4;
    }

    public int getByteLength(String s) {
        try {
            return s.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // We don't have an agent in this method
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not obtain byte length of Java String",e);
            }
        }
        return -1;
    }
    
    

}
