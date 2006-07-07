/*
 *
 * Derby - Class TestInputStream
 *
 * Copyright 2006 The Apache Software Foundation or its
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;
import java.io.InputStream;
import java.io.IOException;

/** 
 * TestInputStream class is a InputStream which returns
 * a lot of data which can be inserted into a LOB.
 */
public final class TestInputStream extends InputStream 
{
    /**
     * Constructor for TestInputStream
     * @param length length of stream
     * @param value value to return
     */
    public TestInputStream(long length, int value) 
    {
        this.value = value;
        this.length = length;
        this.pos = 0;
    }
    
    /**
     * Implementation of InputStream.read(). Returns 
     * the value specified in constructor, unless the 
     * end of the stream has been reached.
     */
    public int read() 
        throws IOException 
    {
        if (++pos>length) return -1;
        return value;
    }
    
    /** Current position in stream */
    private long pos;
    
    /** Value to return */
    final int value;
    
    /** Length of stream */
    final long length;
}
