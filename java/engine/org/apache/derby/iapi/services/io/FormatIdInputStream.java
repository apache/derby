/*

   Derby - Class org.apache.derby.iapi.services.io.FormatIdInputStream

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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassFactoryContext;
import org.apache.derby.iapi.types.Resetable;

import org.apache.derby.iapi.services.context.ContextService;
/**
  A stream for reading objects with format id tags which was
  produced by a FormatIdOutputStream.

  <P>Please see the documentation for FormatIdOutputStream for
  information about the streams format and capabilites.
  */
public final class FormatIdInputStream extends DataInputStream
	 implements ErrorObjectInput, Resetable
{
	protected ClassFactory cf;
	private ErrorInfo errorInfo;
    private Exception myNestedException;


	/**
	  Constructor for a FormatIdInputStream

	  @param in bytes come from here.
	  */
    public FormatIdInputStream(InputStream in)
	{
		super(in);
	}

	/**
	  Read an object from this stream.

	  @return The read object.
	  @exception java.io.IOException An IO or serialization error occured.
	  @exception java.lang.ClassNotFoundException A class for an object in
	  the stream could not be found.
	  */

	public Object readObject() throws IOException, ClassNotFoundException
	{
        setErrorInfo(null);

		int fmtId = FormatIdUtil.readFormatIdInteger(this);

		if (fmtId == StoredFormatIds.NULL_FORMAT_ID)
		{
			return null;
		}

		if (fmtId == StoredFormatIds.STRING_FORMAT_ID)
		{
			return readUTF();
		}

		try
        {

			if (fmtId == StoredFormatIds.SERIALIZABLE_FORMAT_ID)
			{
				ObjectInputStream ois = getObjectStream();
				try {
					Object result = ois.readObject();
					return result;
				} catch (IOException ioe) {
					setErrorInfo((ErrorInfo) ois);
					throw ioe;
				} catch (ClassNotFoundException cnfe) {
					setErrorInfo((ErrorInfo) ois);
					throw cnfe;
				} catch (LinkageError le) {
					setErrorInfo((ErrorInfo) ois);
					throw le;
				} catch (ClassCastException cce) {
					setErrorInfo((ErrorInfo) ois);
					throw cce;
				}
			}

			try {

				Formatable f = (Formatable)Monitor.newInstanceFromIdentifier(fmtId);
				if (f instanceof Storable)
				{
					boolean isNull = this.readBoolean();
					if (isNull == true)
					{
						Storable s = (Storable)f;
						s.restoreToNull();
						return s;
					}
				}

				f.readExternal(this);
				return f;
			} catch (StandardException se) {
				throw new ClassNotFoundException(se.toString());
			}


		}
        catch (ClassCastException cce)
        {
			// We catch this here as it is usuall a user error.
			// they have readExternal (or SQLData) that doesn't match
			// the writeExternal. and thus the object read is of
			// the incorrect type, e.g. Integer i = (Integer) in.readObject();
			throw new StreamCorruptedException(cce.toString());
		}
	}

	/**
	  Set the InputStream for this FormatIdInputStream to the stream
	  provided.

	  @param in The new input stream.
	  */
	public void setInput(InputStream in)
	{
		this.in = in;
	}

    public	InputStream	getInputStream()
    {
        return in;
    }

	public String getErrorInfo()
    {
		if (errorInfo == null)
            return "";

		return errorInfo.getErrorInfo();
	}

    public Exception getNestedException()
    {
        if (myNestedException != null)
            return null;

        if (errorInfo == null)
            return null;

        return errorInfo.getNestedException();
    }

	private void setErrorInfo(ErrorInfo ei)
    {
        errorInfo = ei;
	}


    ClassFactory getClassFactory() {
		if (cf == null) {

			ClassFactoryContext cfc =
				(ClassFactoryContext) ContextService.getContextOrNull
				                                  (ClassFactoryContext.CONTEXT_ID);

			if (cfc != null)
				cf = cfc.getClassFactory();
		}
		return cf;
	}

	/*
	** Class private methods
	*/

	private ObjectInputStream getObjectStream() throws IOException {

		return getClassFactory() == null ?
			new ObjectInputStream(this) :
			new ApplicationObjectInputStream(this, cf);
	}



    /*** Resetable interface ***/

    public void resetStream()
        throws IOException, StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(in instanceof Resetable);
        ((Resetable) in).resetStream();
    }


    public void initStream()
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(in instanceof Resetable);
        ((Resetable) in).initStream();
    }


    public void closeStream()
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(in instanceof Resetable);
        ((Resetable) in).closeStream();
    }

}

