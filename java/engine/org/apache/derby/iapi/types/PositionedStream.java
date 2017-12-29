/*

   Derby - Class org.apache.derby.iapi.types.PositionedStream

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
package org.apache.derby.iapi.types;

import java.io.IOException;
import java.io.InputStream;
import org.apache.derby.shared.common.error.StandardException;

/**
 * This interface describes a stream that is aware of its own position and can
 * reposition itself on request.
 * <p>
 * This interface doesn't convey any information about how expensive it is for
 * the stream to reposition itself.
 */
public interface PositionedStream {

    /**
     * Returns a reference to self as an {@code InputStream}.
     * <p>
     * This method is not allowed to return {@code null}.
     *
     * @return An {@code InputStream} reference to self.
     */
    InputStream asInputStream();

    /**
     * Returns the current byte position of the stream.
     *
     * @return Current byte position of the stream.
     */
    long getPosition();

    /**
     * Repositions the stream to the requested byte position.
     * <p>
     * If the repositioning fails because the stream is exhausted, most likely
     * because of an invalid position specified by the user, the stream is
     * reset to position zero and an {@code EOFException} is thrown.
     *
     * @param requestedPos requested byte position, first position is {@code 0}
     * @throws IOException if accessing the stream fails
     * @throws java.io.EOFException if the requested position is equal to or
     *      larger than the length of the stream
     * @throws StandardException if an error occurs in store
     */
    void reposition(long requestedPos)
            throws IOException, StandardException;
}
