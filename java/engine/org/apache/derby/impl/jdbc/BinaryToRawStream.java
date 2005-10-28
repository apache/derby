/*

   Derby - Class org.apache.derby.impl.jdbc.BinaryToRawStream

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

/**
	Converts a stream containing the Cloudscape stored binary form
	to one that just contains the application's data.
	Simply remove the length information.
*/
final class BinaryToRawStream
extends java.io.FilterInputStream
{

    // used by caller to insure that parent can not be GC'd until this
    // stream is no longer being used.
    private Object          parent;

	public BinaryToRawStream(InputStream in, Object parent) 
        throws IOException
	{
		super(in);

		this.parent     = parent;

		// no need to calculate the actual length
		// int len = 0;
		int bl = in.read();
		if (bl < 0)
			throw new java.io.EOFException();

		if ((bl & 0x80) != 0)
		{
			if (bl == 0xC0)
			{
				int v1 = in.read();
				int v2 = in.read();
				int v3 = in.read();
				int v4 = in.read();

				if (v1 < 0 || v2 < 0 || v3 < 0 || v4 < 0)
					throw new java.io.EOFException();
                //len = (((v1 & 0xff) << 24) | ((v2 & 0xff) << 16) | ((v3 & 0xff) << 8) | (v4 & 0xff));

			}
			else if (bl == 0xA0)
			{
				// read an unsigned short
				int v1 = in.read();
				int v2 = in.read();
				if (v1 < 0 || v2 < 0)
					throw new java.io.EOFException();
                //len = (((v1 & 0xff) << 8) + (v2 & 0xff));

			}
			else
			{
				// len = bl & 0x1F;
			}
		}
		else
		{
			// old length in bits
			int v2 = in.read();
			int v3 = in.read();
			int v4 = in.read();
			if (v2 < 0 || v3 < 0 || v4 < 0)
				throw new java.io.EOFException();
            //int lenInBits = (((bl & 0xff) << 24) | ((v2 & 0xff) << 16) | ((v3 & 0xff) << 8) | (v4 & 0xff));

			//len = lenInBits / 8;
			//if ((lenInBits % 8) != 0)
			//	len++;
		}
	}
}
