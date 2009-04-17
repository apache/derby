/*

   Derby - Class org.apache.derby.iapi.services.io.DataInputUtil

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

package org.apache.derby.iapi.services.io;

import java.io.DataInput;
import java.io.IOException;

/**
 * A util class for DataInput.
 */
public final class DataInputUtil {

    /**
     * Skips requested number of bytes,
     * throws EOFException if there is too few bytes in the DataInput.
     * @param in
     *      DataInput to be skipped.
     * @param skippedBytes
     *      number of bytes to skip. if skippedBytes <= zero, do nothing.
     * @throws EOFException
     *      if EOF meets before requested number of bytes are skipped.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'in' equals null.
     */
    public static void skipFully(DataInput in, int skippedBytes)
    throws IOException {
        if (in == null) {
            throw new NullPointerException();
        }

        while (skippedBytes > 0) {
            int skipped = in.skipBytes(skippedBytes);
            if (skipped == 0) {
                in.readByte();
                skipped++;
            }
            skippedBytes -= skipped;
        }
    }
}
