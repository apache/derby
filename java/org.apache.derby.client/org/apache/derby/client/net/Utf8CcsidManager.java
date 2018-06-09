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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import org.apache.derby.client.am.Agent;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;

public class Utf8CcsidManager extends CcsidManager {

    private final static String UTF8 = "UTF-8";
    private final static Charset UTF8_CHARSET = Charset.forName(UTF8);
    private final CharsetEncoder encoder = UTF8_CHARSET.newEncoder();

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
        try {
            ByteBuffer buf = encoder.encode(CharBuffer.wrap(sourceString));

            if (buf.limit() == buf.capacity()) {
                // The length of the encoded representation of the string
                // matches the length of the returned buffer, so just return
                // the backing array.
                return buf.array();
            }

            // Otherwise, copy the interesting bytes into an array with the
            // correct length.
            byte[] bytes = new byte[buf.limit()];
            buf.get(bytes);
            return bytes;
        } catch (CharacterCodingException cce) {
            throw new SqlException(agent.logWriter_,
                    new ClientMessageId(SQLState.CANT_CONVERT_UNICODE_TO_UTF8),
                    cce);
        }
    }

    /**
     * Offset and numToConvert are given in terms of bytes! Not characters!
     */
    public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
        return new String(sourceBytes, offset, numToConvert, UTF8_CHARSET);
    }

    public void startEncoding() {
        encoder.reset();
    }

    public boolean encode(CharBuffer src, ByteBuffer dest, Agent agent)
            throws SqlException {
        CoderResult result = encoder.encode(src, dest, true);
        if (result == CoderResult.UNDERFLOW) {
            // We've exhausted the input buffer, which means we're done if
            // we just get everything flushed to the destination buffer.
            result = encoder.flush(dest);
        }

        if (result == CoderResult.UNDERFLOW) {
            // Input buffer is exhausted and everything is flushed to the
            // destination. We're done.
            return true;
        } else if (result == CoderResult.OVERFLOW) {
            // Need more room in the output buffer.
            return false;
        } else {
            // Something in the input buffer couldn't be encoded.
            throw new SqlException(agent.logWriter_,
                    new ClientMessageId(SQLState.CANT_CONVERT_UNICODE_TO_UTF8));
        }
    }
}
