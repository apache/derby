/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.services.io.CompressedNumber;

import java.io.InputStream;
import java.io.IOException;

/**
	A DecryptInputStream is used by stream container to access an encrypted
	stream of bytes.
*/
public class DecryptInputStream extends BufferedByteHolderInputStream {

	// if database is encrypted, bytes to reserve at the beginning of the buffer
	//protected static final int ENCRYPTION_RESERVE = dataFactory.getEncryptionBlockSize() - 1;

	protected DataFactory dataFactory;
	protected InputStream in;

	public DecryptInputStream(InputStream in, ByteHolder bh, DataFactory dataFactory)
		throws IOException {

		super(bh);
		this.in = in;
		this.dataFactory = dataFactory;
		fillByteHolder();
	}

	public void fillByteHolder() throws IOException {

		if (this.bh.available() == 0) {

			this.bh.clear();

			try {
				// from the stream, read the actual length of the bytes
				// before it was padded and encrypted.
				int realLen = CompressedNumber.readInt(in);
				// if it is -1, we have reached the end of the file.
				// then we are done.
				if (realLen == -1)
					return;

				// calculate out what the padding was based on the actual length
				int tail = realLen % dataFactory.getEncryptionBlockSize();
				int padding = (tail == 0) ? 0 : (dataFactory.getEncryptionBlockSize() - tail);
				int encryptedLen = realLen + padding;

				// read all encrypted data including the padding.
				byte[] ciphertext = new byte[encryptedLen];
				in.read(ciphertext, 0, encryptedLen);
				byte[] cleartext = new byte[encryptedLen];
				// decrypt the data, and stored it in a new byte array.
				dataFactory.decrypt(ciphertext, 0, encryptedLen, cleartext, 0);

				// only put the actual data without the padding into the byte holder.
				bh.write(cleartext, padding, realLen);

			} catch (StandardException se) {
				throw new IOException();
			}

			// allow reading from the byte holder.
			this.bh.startReading();
		}
	}
}
