/*
    Derby - Class org.apache.derby.client.net.PublicBufferOutputStream

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
*/
package org.apache.derby.client.net;

import java.io.ByteArrayOutputStream;

/**
 * A ByteArrayOutputStream which gives a direct reference of the buffer array
 */
class PublicBufferOutputStream extends ByteArrayOutputStream {

    PublicBufferOutputStream() {
        super();
    }

    public PublicBufferOutputStream(int size) {
        super(size);
    }

    /**
     * Get a reference to the buffer array stored in the byte array
     * output stream
     */
    public byte[] getBuffer() {
        return buf;
    }

}
