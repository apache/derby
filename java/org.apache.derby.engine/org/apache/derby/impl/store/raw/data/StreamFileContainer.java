/*

   Derby - Class org.apache.derby.impl.store.raw.data.StreamFileContainer

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StreamStorable;
import org.apache.derby.iapi.services.io.FormatIdInputStream;
import org.apache.derby.iapi.services.io.FormatIdOutputStream;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.TypedFormat;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;

import org.apache.derby.io.StorageFile;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.CompressedNumber;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.util.Properties;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InvalidClassException;
import java.io.Externalizable;
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.io.FileNotFoundException;

/**

  The format of this stream file is:
  (RH) (FH) (field data) (FH) (field data) ........ (FH) (field data)

  Record header is stored once at the beginning of the file
  for all the rows stored in this file.
  Record Header indicates how many fields are in each row.
  Then we just stored all the column from each row.
  Field header stored on this file is fixed size with fieldDataLength
  size set to LARGE_SLOT_SIZE (4) bytes.

  NOTE: No locks are used in this container.  All transaction are not logged.

**/


class StreamFileContainer implements TypedFormat, PrivilegedExceptionAction<Object>
{

    /**************************************************************************
     * Constant Fields of the class
     **************************************************************************
     */

	/*
	 * typed format
	 * format Id must fit in 4 bytes
	 */
	protected static int formatIdInteger = 
        StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_STREAM_FILE; 


    // 4 bytes for field data length
	protected static final int LARGE_SLOT_SIZE = 4;	

	protected static final int MIN_BUFFER_SIZE = 
        RawStoreFactory.STREAM_FILE_BUFFER_SIZE_MINIMUM;
	protected static final int FIELD_STATUS = 
        StoredFieldHeader.setFixed(StoredFieldHeader.setInitial(), true);
	protected static final int FIELD_HEADER_SIZE = 
        StoredFieldHeader.size(FIELD_STATUS, 0, LARGE_SLOT_SIZE);


    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
	protected   ContainerKey          identity;
	private   BaseDataFileFactory   dataFactory;  // the factory that made me

	private int                             bufferSize;

	private StorageFile file;

	private OutputStream                fileOut;
	private DynamicByteArrayOutputStream    out;
	private FormatIdOutputStream            logicalDataOut;

	private InputStream                 fileIn;
	private BufferedInputStream             bufferedIn;
	private DecryptInputStream              decryptIn;
	private LimitInputStream                limitIn;
	private FormatIdInputStream             logicalDataIn;

	private StoredRecordHeader              recordHeader;

	private byte[]                          ciphertext;
	private byte[]                          zeroBytes;	// in case encryption
                                                        // stream needs pad.


    /* privileged actions */
    private static final int STORAGE_FILE_EXISTS_ACTION = 1;
    private static final int STORAGE_FILE_DELETE_ACTION = 2;
    private static final int STORAGE_FILE_MKDIRS_ACTION = 3;
    private static final int STORAGE_FILE_GET_OUTPUT_STREAM_ACTION = 4;
    private static final int STORAGE_FILE_GET_INPUT_STREAM_ACTION = 5;
    private int actionCode;
    private StorageFile actionStorageFile;


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * Constructor.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	StreamFileContainer(
    ContainerKey        identity, 
    BaseDataFileFactory dataFactory)
    {
		this.identity = identity;
		this.dataFactory = dataFactory;
	}

    /**
     * Constructor
     * <p>
     * when rowSource is passed to the constructor, it will be loaded into the 
     * container after the container has been created.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	StreamFileContainer(
    ContainerKey        identity, 
    BaseDataFileFactory dataFactory,
    Properties          prop)
		throws StandardException 
    {
		this.identity       = identity;
		this.dataFactory    = dataFactory;

		try 
        {
			file = getFileName(identity, true, false);

            if (privExists(file)) 
            {
				// note I'm left in the no-identity state as fillInIdentity()
                // hasn't been called.
				throw StandardException.newException(
                        SQLState.FILE_EXISTS, file);
			}

			// get the properties to set buffer size
			// derby.storage.streamFileBufferSize
			getContainerProperties(prop);

		} 
        catch (SecurityException se) 
        {
			throw StandardException.newException(
                    SQLState.FILE_CREATE, se, file);
		}
	}

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**
     * Open a stream file container.
     * <p>
     * Open a container. Open the file that maps to this container, if the
     * file does not exist then we assume the container was never created
     * and return.
     * If the file exists but we have trouble opening it then we throw some 
     * exception.
     * <p>
     *
	 * @return The opened StreamFileContainer.
     *
     * @param forUpdate     Currently only accepts false, updating and existing
     *                      stream file container is not currently supported.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected StreamFileContainer open(boolean forUpdate) 
        throws StandardException 
    {

		file = getFileName(this.identity, false, true);
        if (!privExists(file))
			return null;

		try 
        {
			if (!forUpdate) 
            {
				fileIn = privGetInputStream(file);

				if (dataFactory.databaseEncrypted()) 
                {
					// if the database is encrypted, when reading the data back
                    // from the file stream, we need to used the decrypt stream
                    // to buffer up the bytes for reading.  DecryptInputStream 
                    // also decrypts the data.

					MemByteHolder byteHolder = 
                        new MemByteHolder(
                            RawStoreFactory.STREAM_FILE_BUFFER_SIZE_DEFAULT);

					decryptIn = 
                        new DecryptInputStream(fileIn, byteHolder, dataFactory);

					limitIn = new LimitInputStream(decryptIn);
				} 
                else 
                {
					bufferedIn = 
                        new BufferedInputStream(
                            fileIn, 
                            RawStoreFactory.STREAM_FILE_BUFFER_SIZE_DEFAULT);

					limitIn = new LimitInputStream(bufferedIn);
				}

				// the logicalDataIn input stream is on top of a limit Input
				// stream, use a limit stream to make sure we don't read off
				// more then what each column says it contains

				logicalDataIn = new FormatIdInputStream(limitIn);

				// get the record header
				recordHeader = new StoredRecordHeader();
				recordHeader.read(logicalDataIn);

			} 
            else 
            {
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT(
                        "updating existing stream container not supported yet");

				return null;
			}
		} 
        catch (IOException ioe) 
        {
			throw StandardException.newException(
                    SQLState.FILE_CREATE, ioe, file);
		}

		return this;
	}

    /**
     * Close the stream file.
     * <p>
     * Close this stream file, and all streams associated with it.
     * <p>
     **/
	protected void close()
    {
		try 
        {

			if (fileIn != null) 
            {
				fileIn.close();
				fileIn = null;
				if (dataFactory.databaseEncrypted()) 
                {
					decryptIn.close();
					decryptIn = null;
				} 
                else 
                {
					bufferedIn.close();
					bufferedIn = null;
				}
				logicalDataIn.close();
				logicalDataIn = null;
			}

			if (fileOut != null) 
            {
				fileOut.close();
				logicalDataOut.close();
				fileOut = null;
				logicalDataOut = null;
				out = null;
			}

		} 
        catch (IOException ioe) 
        {
            // ignore close errors from fileOut.close() and fileIn.close() - 
            // there isn't much we can do about them anyway - and some of the
            // interfaces don't want to deal with exceptions from close().

            /*
			throw StandardException.newException(
                    SQLState.FILE_CREATE, ioe, file);
            */
		}
	}

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Return my format identifier.
     **/
	public int getTypeFormatId() 
    {
		return StoredFormatIds.RAW_STORE_SINGLE_CONTAINER_STREAM_FILE;
	}

    /**
     * Request the system properties associated with a stream container. 
     * <p>
     * Request the value of properties associated with a stream container. 
     * The following properties can be requested:
     *     derby.storage.streamFileBufferSize
	 *
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.streamFileBufferSize", "");
     *     cc.getContainerProperties(prop);
     *
     *     System.out.println(
     *         "stream table's buffer size = " + 
     *         prop.getProperty("derby.storage.streamFileBufferSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void getContainerProperties(Properties prop) 
        throws StandardException 
    {

		AccessFactory af = (AccessFactory)
			getServiceModule(dataFactory, AccessFactory.MODULE);

		TransactionController tc = 
            (af == null) ? 
                null : 
                af.getTransaction(
                    getContextService().getCurrentContextManager());

		bufferSize = 
			PropertyUtil.getServiceInt(tc, prop,
				RawStoreFactory.STREAM_FILE_BUFFER_SIZE_PARAMETER,  
				RawStoreFactory.STREAM_FILE_BUFFER_SIZE_MINIMUM, 
				RawStoreFactory.STREAM_FILE_BUFFER_SIZE_MAXIMUM,
				RawStoreFactory.STREAM_FILE_BUFFER_SIZE_DEFAULT);
	}

    /**
     * Request the container key associated with the stream container. 
	 **/
	public ContainerKey getIdentity() 
    {
		return this.identity;
	}

    /**
     * Can I use this container?
     * <p>
     * This method always return true right now.
     * In the future when there are different uses for this container,
     * we may need to add qualifications for this.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected boolean use(StreamContainerHandle handle) 
        throws StandardException 
    {
		return true;
	}

    /**
     * load data into this container.
     * <p>
     * populate the stream container with data in the rowSource
     * <p>
     *
     * @param rowSource The row source to get rows to load into this container.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void load(RowSource rowSource) 
        throws StandardException 
    {
		// use this output stream to buffer rows before inserting into file.
		out                 = new DynamicByteArrayOutputStream(bufferSize);
		logicalDataOut      = new FormatIdOutputStream(out);
		boolean encrypted   = dataFactory.databaseEncrypted();

		// reserve the first dataFactory.getEncryptionBlockSize() - 1 bytes, if the database is 
        // encrypted These reserved bytes will be used to pad the byte array if
        // it is not dataFactory.getEncryptionBlockSize() aligned.
		if (encrypted) 
        {
			if (zeroBytes == null)
				zeroBytes = new byte[dataFactory.getEncryptionBlockSize() - 1];

			out.write(zeroBytes, 0, dataFactory.getEncryptionBlockSize() - 1);
		}

		try 
        {
			fileOut = privGetOutputStream(file);

			FormatableBitSet validColumns = rowSource.getValidColumns();

			Object[] row = rowSource.getNextRowFromRowSource();

			int numberFields = 0;
			if (validColumns != null) 
            {
				for (int i = validColumns.getLength() - 1; i >= 0; i--) 
                {
					if (validColumns.isSet(i)) 
                    {
						numberFields = i + 1;
						break;
					}
				}
			} 
            else 
            {
				numberFields = row.length;
			}

			// make the record header to have 0 record id
			recordHeader = new StoredRecordHeader(0, numberFields);

			// write the record header once for all the rows, directly to the 
            // beginning of the file.
			int rhLen = recordHeader.write(out);

			int validColumnsSize = 
                validColumns == null ? 0 : validColumns.getLength();

			while (row != null) 
            {

				int arrayPosition = -1;

				for (int i = 0; i < numberFields; i++) 
                {

					// write each column out
					if (validColumns == null) 
                    {
						arrayPosition++;
						Object column = row[arrayPosition];
						writeColumn(column);
					} 
                    else 
                    {

						if (validColumnsSize > i && validColumns.isSet(i)) 
                        {
							arrayPosition++;
							Object column = row[arrayPosition];
							writeColumn(column);
						} 
                        else 
                        {
							// it is a non-existent column
							writeColumn(null);
						}
					}

					// put the buffer onto the page, only if it exceeded the 
                    // original buffer size or it has less than 100 bytes left 
                    // in the buffer
					if ((out.getUsed() >= bufferSize) || 
                        ((bufferSize - out.getUsed()) < MIN_BUFFER_SIZE)) 
                    {
						writeToFile();
					}
				}

				// get the next row and its valid columns from the rowSource
				row = rowSource.getNextRowFromRowSource();
			}


			// Write the buffer to the file if there is something in the output
			// buffer.  Remember we pad the output buffer with
			// dataFactory.getEncryptionBlockSize() - 1 if this is an encypted database
			if (encrypted)
			{
				if (out.getUsed() > (dataFactory.getEncryptionBlockSize() - 1))
					writeToFile();
			}
			else if (out.getUsed() > 0) 
            {
				writeToFile();
			}

		} 
        catch (IOException ioe) 
        {
			// handle IO error...
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);

		}
        finally 
        {
			close();
		}
	}

	/*

	 */
    /**
     * Write the buffer to the file.
     * <p>
     * If the database is encrypted, the dataFactory.getEncryptionBlockSize() - 1 reserved bytes will
     * be used to pad the byte array to be dataFactory.getEncryptionBlockSize()
     * aligned.  Before the bytes are encrypted and written to the file stream,
     * the actual length of the byte array is written out as a compressed 
     * integer.  This number will be used when decrypting the data.
     *
     * If the database is not encrypted, then, we don't reserve the bytes 
     * upfront, and we simple just write the bytes out to the file stream.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void writeToFile() 
        throws StandardException 
    {

		try
        {
			if (dataFactory.databaseEncrypted()) 
            {
				// if db is encrypted, 
                // use the first ENCRYPTION_ALIGN bytes for padding.
                //
				int realLen = out.getUsed() - (dataFactory.getEncryptionBlockSize() - 1);
				int tail = realLen % dataFactory.getEncryptionBlockSize();
				int padding = 
                    (tail == 0) ? 0 : 
                    (dataFactory.getEncryptionBlockSize() - tail);

				int startByte = (tail == 0) ? (dataFactory.getEncryptionBlockSize() - 1) : (tail - 1);
				int encryptedLen = realLen + padding;

				// there is nothing to write, just the encryption padding
				if (realLen <= 0)
					return;

				if (ciphertext == null)
                {
					ciphertext = new byte[encryptedLen];
                }
				else 
                {
					if (ciphertext.length < encryptedLen)
						ciphertext = new byte[encryptedLen];
				}

				dataFactory.encrypt(
                    out.getByteArray(), startByte, encryptedLen, ciphertext, 0, false);

				// write out the actual length, then the encrypted bytes.
				CompressedNumber.writeInt(fileOut, realLen);
				dataFactory.writeInProgress();
				try
				{
					fileOut.write(ciphertext, 0, encryptedLen);
				}
				finally
				{
					dataFactory.writeFinished();
				}

				// reset the dynamic buffer
				out.reset();

				// reserve bytes if database is encrypted.
				if (dataFactory.databaseEncrypted())
                {
					if (zeroBytes == null)
						zeroBytes = new byte[dataFactory.getEncryptionBlockSize() - 1];

					out.write(zeroBytes, 0, dataFactory.getEncryptionBlockSize() - 1);
				}

			} 
            else 
            {
				// nothing to write
				if (out.getUsed() == 0)
					return;

				dataFactory.writeInProgress();
				try
				{
					fileOut.write(out.getByteArray(), 0, out.getUsed());
				}
				finally
				{
					dataFactory.writeFinished();
				}

				// reset the dynamic buffer
				out.reset();
			}
		} 
        catch (IOException ioe) 
        {
			throw StandardException.newException(
                    SQLState.DATA_UNEXPECTED_EXCEPTION, ioe);
		}
	}

	private void writeColumn(Object column) 
        throws StandardException, IOException 
    {

		int fieldStatus = FIELD_STATUS;
		if (column == null) 
        {
			// just write a non-existent header.
			fieldStatus = StoredFieldHeader.setNonexistent(fieldStatus);
			StoredFieldHeader.write(out, fieldStatus, 0, LARGE_SLOT_SIZE);
			return;
		}

		// if the column is a null column, write the field header now.
		if (column instanceof Storable) 
        {
			Storable sColumn = (Storable) column;
			if (sColumn.isNull()) 
            {
				fieldStatus = StoredFieldHeader.setNull(fieldStatus, true);
				StoredFieldHeader.write(out, fieldStatus, 0, LARGE_SLOT_SIZE);
				return;
			}
		}

		int beginPosition = out.getPosition();
		int fieldDataLength = 0;

		// write out the header, mostly to reserve the space
		StoredFieldHeader.write(
            out, fieldStatus, fieldDataLength, LARGE_SLOT_SIZE);

		if (column instanceof StreamStorable) 
        {
			if (((StreamStorable) column).returnStream() != null) 
            {
				column = (InputStream) ((StreamStorable) column).returnStream();
			}
		}

		if (column instanceof InputStream) 
        {
			InputStream inColumn = (InputStream) column;
            // Set a reasonable buffer size.
            // To avoid extremely inefficient reads, and an infinite loop when
            // InputStream.available() returns zero, a lower limit is set on
            // the buffer size. To avoid using too much memory (especially in
            // multi-user environments) an upper limit is set as well.
            // The limits can be tuned, but note that using a too high default
            // or lower limit can put unnecessary pressure on the memory sub-
            // system and the GC process.
            int bufferLen = Math.min(Math.max(inColumn.available(), 64), 8192);
			byte[] bufData = new byte[bufferLen];

			do 
            {
                int lenRead = inColumn.read(bufData);
				if (lenRead != -1) 
                {
					fieldDataLength += lenRead;
                    out.write(bufData, 0, lenRead);
				} 
                else
                {
					break; 
                }
			} while (true);

		} 
        else if (column instanceof Storable) 
        {

			Storable sColumn = (Storable) column;
			// write field data to the stream, we already handled the null case
            
			sColumn.writeExternal(logicalDataOut);
			fieldDataLength = 
                out.getPosition() - beginPosition - FIELD_HEADER_SIZE;

		} 
        else 
        {
			// Serializable/Externalizable/Formattable
			// all look the same at this point.
			logicalDataOut.writeObject(column);
			fieldDataLength = 
                out.getPosition() - beginPosition - FIELD_HEADER_SIZE;
		}

		// Now we go back to update the fieldDataLength in the field header
		int endPosition = out.getPosition();
		out.setPosition(beginPosition);

		StoredFieldHeader.write(
            out, fieldStatus, fieldDataLength, LARGE_SLOT_SIZE);

		// set position to the end of the field
		if (!StoredFieldHeader.isNull(fieldStatus))
			out.setPosition(endPosition);
	}

	public boolean fetchNext(Object[] row) 
        throws StandardException 
    {

		boolean inUserCode = false;
		int columnId = 0;
		
		try	
        {

			// Get the number of columns in the row.
			int numberFields = recordHeader.getNumberFields();

			int arrayPosition = 0;
			for (columnId = 0; columnId < numberFields; columnId++) 
            {

				if (arrayPosition >= row.length)
					break;
	
				limitIn.clearLimit();

				// read the field header
				int fieldStatus = StoredFieldHeader.readStatus(logicalDataIn);
				int fieldDataLength = StoredFieldHeader.readFieldDataLength(
					logicalDataIn, fieldStatus, LARGE_SLOT_SIZE);

				limitIn.setLimit(fieldDataLength);

				if (SanityManager.DEBUG) 
                {

                    if (StoredFieldHeader.isExtensible(fieldStatus))
                    {
                        SanityManager.THROWASSERT(
                            "extensible fields not supported yet.  columnId = "
                            + columnId);
                    }

					SanityManager.ASSERT(!StoredFieldHeader.isOverflow(fieldStatus),
						"overflow field is not supported yet");
				}

				Object column = row[arrayPosition];
				
				// Deal with Storable columns
				if (StoredFieldHeader.isNullable(fieldStatus)) 
                {
									
					if (column == null)
                    {
						throw StandardException.newException(
                                SQLState.DATA_NULL_STORABLE_COLUMN, 
                                Integer.toString(columnId));
                    }

					// SRW-DJD RESOLVE: - fix error message
					if (!(column instanceof Storable)) 
                    {
						throw StandardException.newException(
                            SQLState.DATA_NULL_STORABLE_COLUMN, 
                            column.getClass().getName());
					}

					Storable sColumn = (Storable) column;

					// is the column null ?
					if (StoredFieldHeader.isNull(fieldStatus)) 
                    {

						sColumn.restoreToNull();
						arrayPosition++;
						continue;
					}

					inUserCode = true;
					sColumn.readExternal(logicalDataIn);
					inUserCode = false;
					arrayPosition++;
					continue;
				}

				// Only Storables can be null ... SRW-DJD RESOLVE: - fix error message
				if (StoredFieldHeader.isNull(fieldStatus))
                {
					throw StandardException.newException(
                        SQLState.DATA_NULL_STORABLE_COLUMN, 
                        Integer.toString(columnId));
                }

				// This is a non-extensible field, which means the caller must 
                // know the correct type and thus the element in row is the 
                // correct type or null. If the element implements 
                // Externalizable then we can just fill it in, otherwise it 
                // must be Serializable and we have to throw it away.

				Object neColumn = row[arrayPosition];

				if (neColumn instanceof Externalizable) 
                {

					Externalizable exColumn = (Externalizable) neColumn;

					inUserCode = true;
					exColumn.readExternal(logicalDataIn);
					inUserCode = false;

					arrayPosition++;
					continue;
				}

				// neColumn will be ignored
				neColumn = null;
				inUserCode = true;
				row[arrayPosition] = logicalDataIn.readObject();
				inUserCode = false;

				arrayPosition++;
				continue;
			}

		} 
        catch (IOException ioe) 
        {

			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable
			if (inUserCode) 
            { 

				if (ioe instanceof EOFException) 
                {
					throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_MISMATCH, 
                        ioe, logicalDataIn.getErrorInfo());
				}

				throw StandardException.newException(
                    SQLState.DATA_STORABLE_READ_EXCEPTION, 
                    ioe, logicalDataIn.getErrorInfo());
			}

			if (ioe instanceof InvalidClassException)
            {
				throw StandardException.newException(
                        SQLState.DATA_STORABLE_READ_EXCEPTION, 
                        ioe, logicalDataIn.getErrorInfo());
   			}

			// If we are at the end of the file, trying to fetch the first 
            // column, then we know there is no more rows to fetch
			if ((ioe instanceof EOFException) && (columnId == 0)) 
            {
				close();
				return false;
			}

			throw dataFactory.markCorrupt(
                StandardException.newException(
                    SQLState.DATA_CORRUPT_STREAM_CONTAINER, ioe, identity));

		} 
        catch (ClassNotFoundException cnfe) 
        {

			if (SanityManager.DEBUG) 
            {
				SanityManager.ASSERT(inUserCode);
			}

			// an exception during the restore of a user column, this doesn't
			// make the database corrupt, just that this field is inaccessable
			throw StandardException.newException(
                SQLState.DATA_STORABLE_READ_MISSING_CLASS, cnfe, 
                logicalDataIn.getErrorInfo());

		} 
        catch (LinkageError le) 
        {
			if (inUserCode)
            {
				throw StandardException.newException(
                    SQLState.DATA_STORABLE_READ_EXCEPTION, le,
                    logicalDataIn.getErrorInfo());
            }
			throw le;
		}

		return true;

	}

    /**
     * Close the stream file and remove the file.
     *
     * @exception StandardException Segment directory cannot be created
     **/
	public boolean removeContainer() 
        throws StandardException 
    {
		close();

        if (privExists(file))
        {
            return privDelete(file);
        }
        else
        {
            return true;
        }


	}

    /**
     * Return a file name for the identity.
     * <p>
     * Return a valid file name for the identity, or null if the data
     * directory for this segment cannot be created
     *
	 * @exception StandardException Segment directory cannot be created
     **/
	protected StorageFile getFileName(
    ContainerKey    identity, 
    boolean         forCreate, 
    boolean         errorOK)
		 throws StandardException 
    {
		if (identity.getSegmentId() == StreamContainerHandle.TEMPORARY_SEGMENT)
        {
			return( dataFactory.storageFactory.newStorageFile( dataFactory.storageFactory.getTempDir(), 
                    "T" + identity.getContainerId() + ".tmp"));
        }
		else 
        {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT(
                    "cannot create stream container in non-temp segments yet.");

			StorageFile container = dataFactory.getContainerPath( identity, false);

			if (!privExists(container)) 
            {

				if (!forCreate)
					return null;

				StorageFile directory = container.getParentDir();

				if (!privExists(directory)) 
                {
					// make sure only 1 thread can create a segment at one time
					synchronized(dataFactory) 
                    {
						if (!privExists(directory)) 
                        {
                            boolean created = false;
                            IOException ex = null;
                            try {
                                created = privMkdirs(directory);
                            } catch (IOException ioe) {
                                ex = ioe;
                            }

                            if (!created)
                            {
								if (errorOK)
									return null;
								else
									throw StandardException.newException(
                                            SQLState.FILE_CANNOT_CREATE_SEGMENT,
                                            ex, directory);
							}
						}
					}
				}
			}
			return container;
		}
	}



    
    private synchronized boolean privExists(StorageFile file)
    {
        actionCode = STORAGE_FILE_EXISTS_ACTION;
        actionStorageFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }catch( PrivilegedActionException pae) 
        { 
            // method executed under this priveleged block 
            // does not throw an exception
            return false;
        } 
        finally
        {
            actionStorageFile = null;
        }
    }

    private synchronized boolean privMkdirs(StorageFile file)
            throws IOException
    {
        actionCode = STORAGE_FILE_MKDIRS_ACTION;
        actionStorageFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }catch( PrivilegedActionException pae) 
        {
            // method executed under this priveleged block 
            // could throw IOException
            throw (IOException) pae.getCause();
        } 
        finally
        {
            actionStorageFile = null;
        }
    }

    
    private synchronized boolean privDelete(StorageFile file)
    {
        actionCode = STORAGE_FILE_DELETE_ACTION;
        actionStorageFile = file;

        try
        {
            Object ret = AccessController.doPrivileged( this);
            return ((Boolean) ret).booleanValue();
        }catch( PrivilegedActionException pae) 
        { 
            // method executed under this priveleged block 
            // does not throw an exception
            return false;
        } 
        finally
        {
            actionStorageFile = null;
        }
    }

    private synchronized OutputStream privGetOutputStream(StorageFile file)
        throws FileNotFoundException
    {
        actionCode = STORAGE_FILE_GET_OUTPUT_STREAM_ACTION;
        actionStorageFile = file;

        try
        {
            return (OutputStream) AccessController.doPrivileged( this);
        }catch( PrivilegedActionException pae) 
        { 
            throw (FileNotFoundException)pae.getException();
        } 
        finally
        {
            actionStorageFile = null;
        }
    }


    private synchronized InputStream privGetInputStream(StorageFile file)
        throws FileNotFoundException
    {
        actionCode = STORAGE_FILE_GET_INPUT_STREAM_ACTION;
        actionStorageFile = file;

        try
        {
            return (InputStream) AccessController.doPrivileged( this);
        }catch( PrivilegedActionException pae) 
        { 
            throw (FileNotFoundException)pae.getException();
        } 
        finally
        {
            actionStorageFile = null;
        }
    }


    // PrivilegedAction method
    public Object run() throws IOException
    {
        switch(actionCode)
        {
        case STORAGE_FILE_EXISTS_ACTION:
            return actionStorageFile.exists();
        case STORAGE_FILE_DELETE_ACTION:
            return actionStorageFile.delete();
        case STORAGE_FILE_MKDIRS_ACTION:
            boolean created = actionStorageFile.mkdirs();
            actionStorageFile.limitAccessToOwner();
            return created;
        case STORAGE_FILE_GET_OUTPUT_STREAM_ACTION:
            return actionStorageFile.getOutputStream();
        case STORAGE_FILE_GET_INPUT_STREAM_ACTION:
            return actionStorageFile.getInputStream();
        }

        return null;
    }

    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
                     return ContextService.getFactory();
                 }
             }
             );
    }

    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getServiceModule( final Object serviceModule, final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getServiceModule( serviceModule, factoryInterface );
                 }
             }
             );
    }

}
