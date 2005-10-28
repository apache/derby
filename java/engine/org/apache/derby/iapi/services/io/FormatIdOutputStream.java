/*

   Derby - Class org.apache.derby.iapi.services.io.FormatIdOutputStream

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.info.JVMInfo;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;


/**
  A stream for serializing objects with format id tags.

  <P>An ObjectOutput (henceforth 'out') preceeds objects it writes with
  a format id. The companion FormatIdInputStream (henceforth 'in')
  uses these format ids in parsing the stored data. The stream
  can be thought of as containing a sequence of (formatId,object) pairs
  interspersed with other data. The assumption is that out.writeObject()
  produces these pairs and in.readObject() uses the format ids to
  construct objects from the pairs that out.writeObject produced.
  The description below describes each supported pair and how in.readObject()
  processes it.

  <OL>
  <LI> (NULL_FORMAT_ID, nothing) in.readObject() returns null.
  <LI> (SRING_FORMAT_ID, UTF8 encoded string)in.readObject reads and
       returns this string.
  <LI> (SERIALIZABLE_FORMAT_ID,serialized object) in.readObject() reads
       the object using java serialization and returns it.
  <LI> (A format id for a Storable, isNull flag and object if isNull == false)
       (see note 1) in.readObject() reads the boolean isNull flag. If is null
	   is true, in.readObject() returns a Storable object of the correct
	   class which is null. If ifNull is false, in.readObject() restores
	   the object using its readExternal() method.
  <LI> (A format id for a Formatable which is not Storable, the stored object)
       (see note 1) in.readObject restores the object using its
	   readExternal() method.
  </OL>

  <P>Note 1: The FormatIdInputStream uses
  Monitor.newInstanceFromIdentifier(format id) to get the class.
  <P>Note 2: An object may support more than one of the following
  interfaces Storable, Formatable, Serializable. In this case out.writeObject
  use the first of these interfaces which the object supports (based on the order
  listed here) to determine how to write the object.
 */
public class FormatIdOutputStream
extends DataOutputStream implements ObjectOutput, ErrorInfo
{

	/**
	  Constructor for a FormatIdOutputStream

	  @param out output goes here.
	  */
	public FormatIdOutputStream(OutputStream out)
	{
		super(out);
	}

	/**
	  Write a format id for the object provied followed by the
	  object itself to this FormatIdOutputStream.

	  @param ref a reference to the object.
	  @exception java.io.IOException the exception.
	  */
	public void writeObject(Object ref) throws IOException
	{
		if (ref == null)
		{
			FormatIdUtil.writeFormatIdInteger(this, StoredFormatIds.NULL_FORMAT_ID);
			return;
		}

		if (ref instanceof String)
		{
            // String's are special cased to use writeUTF which is more
            // efficient than the default writeObject(String), but the format
            // can only store 65535 bytes.  The worst case size conversion is
            // 3 bytes for each unicode character in a String, so limiting
            // writeUTF optimization to strings smaller than 20000 should 
            // insure that we won't call writeUTF() and produce more than
            // 65535 bytes.

            String  str = (String) ref;

            if (str.length() <= 20000)
            {
                FormatIdUtil.writeFormatIdInteger(
                    this, StoredFormatIds.STRING_FORMAT_ID);

                this.writeUTF((String)ref);
                return;
            }
		}

		// Add debugging code to read-in every formatable that we write
		// to ensure that it can be read and it's correctly registered.
		OutputStream oldOut = null;
		if (SanityManager.DEBUG) {

			if (ref instanceof Formatable) {

				oldOut = this.out;

				this.out = new DebugByteTeeOutputStream(oldOut);
			}
		}

		if (ref instanceof Storable)
        {
			Storable s = (Storable)ref;

			int fmtId = s.getTypeFormatId();

			if (fmtId != StoredFormatIds.SERIALIZABLE_FORMAT_ID) {
				FormatIdUtil.writeFormatIdInteger(this, fmtId);
				boolean isNull = s.isNull();
				writeBoolean(isNull);
				if (!isNull)
				{
					s.writeExternal(this);
				}
				if (SanityManager.DEBUG) {
					((DebugByteTeeOutputStream) this.out).checkObject(s);
					this.out = oldOut;
				}
				return;
			}
		}
		else if (ref instanceof Formatable)
		{
			Formatable f =
				(Formatable) ref;
			int fmtId = f.getTypeFormatId();

			if (fmtId != StoredFormatIds.SERIALIZABLE_FORMAT_ID) {
				FormatIdUtil.writeFormatIdInteger(this,fmtId);
				f.writeExternal(this);

				if (SanityManager.DEBUG) {
					((DebugByteTeeOutputStream) this.out).checkObject(f);
					this.out = oldOut;
				}
				return;
			}
		}

		/*
		** Otherwise we assume (ref instanceof Serializable).
		** If it isn't we'll get an error, which is what
	 	** we would expect if someone uses something that
	 	** doesn't support Serializable/Externalizable/Formattable
		** when it should.
		*/
		{

			/*
			** If we are debugging (SerializeTrace), we are
			** going to print out every unexpected serialized
			** class.  We print them out to stdout to help
			** in debugging (so they cause diffs in test runs).
			** This is only active in a SANE server.
			*/
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("SerializedTrace"))
				{
					String name = ref.getClass().getName();
					if (
						!name.startsWith("java.lang") &&
						!name.startsWith("java.math"))
					{
						SanityManager.DEBUG("SerializedTrace",
							"...writing serialized class: "+name);
						System.out.println(
							"...writing serialized class: "+name);
					}
				}
			}

			FormatIdUtil.writeFormatIdInteger(this, StoredFormatIds.SERIALIZABLE_FORMAT_ID);
			ObjectOutputStream oos = new ObjectOutputStream(this);
			oos.writeObject(ref);
			oos.flush();

			if (SanityManager.DEBUG && ref instanceof Formatable) {
				((DebugByteTeeOutputStream) this.out).checkObject((Formatable) ref);
				this.out = oldOut;
			}
		}
	}

	/**
	  Set the OutputStream for this FormatIdOutputStream to the stream
	  provided. It is the responsibility of the caller to flush or
	  close (as required) the previous stream this class was attached to.

	  @param out The new output stream.
	  */
	public void setOutput(OutputStream out)
	{
		this.out = out;
		this.written = 0;
	}


    /* Methods of ErrorInfo, used here for SQLData error reporting */

	public String getErrorInfo()
    {
        return null;
    }

    public Exception getNestedException()
    {
          return null;
    }

}
