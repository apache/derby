/*

   Derby - Class org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
	A DynamicByteArrayOutputStream allows writing to a dynamically resizable
	array of bytes.   In addition to dynamic resizing, this extension allows
	the user of this class to have more control over the position of the stream
	and can get a direct reference of the array.
*/
public class DynamicByteArrayOutputStream extends org.apache.derby.shared.common.io.DynamicByteArrayOutputStream
{
	public DynamicByteArrayOutputStream() { super(); }
	public DynamicByteArrayOutputStream(int size) { super( size ); }
	public DynamicByteArrayOutputStream(byte[] data) { super( data ); }
	public DynamicByteArrayOutputStream(DynamicByteArrayOutputStream toBeCloned) { super( toBeCloned ); }
}
