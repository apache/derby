/*

   Derby - Class org.apache.derby.impl.services.stream.SingleStream

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.stream;

import org.apache.derby.iapi.services.stream.InfoStreams;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.Writer;

import java.util.Properties;

import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Member;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * The Basic Services provide InfoStreams for reporting
 * information. Two streams are provided: trace and error.
 * It is configurable where these streams are directed.
 * <p>
 * Errors will be printed to the error stream in addition
 * to being sent to the client.
 * <p>
 * By default both streams are sent to an error log
 * for the system. When creating a message for a stream,
 * you can create an initial entry with header information
 * and then append to it as many times as desired.
 * <p>
 * Note: if character encodings are needed, the use of
 * java.io.*OutputStream's should be replaced with
 * java.io.*Writer's (assuming the Writer interface
 * remains stable in JDK1.1)
 *
 * @author ames
 */
public class SingleStream
implements InfoStreams, ModuleControl, java.security.PrivilegedAction
{

	/*
	** Instance fields
	*/
	private HeaderPrintWriter theStream;


	/**
		  The no-arg public constructor for ModuleControl's use.
	 */
	public SingleStream() {
	}

	/**
	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#boot
	 */
	public void boot(boolean create, Properties properties) {
		theStream = makeStream();
	}


	/**
	 * @see org.apache.derby.iapi.services.monitor.ModuleControl#stop
	 */
	public void stop()	{
		((BasicHeaderPrintWriter) theStream).complete();
	}

	/*
	 * InfoStreams interface
	 */

	/**
	 * @see org.apache.derby.iapi.services.stream.InfoStreams#stream
	 */
	public HeaderPrintWriter stream() {
		return theStream;
	}

	//
	// class interface
	//

	/**
		Make the stream; note that service properties override
		application and system properties.

	 */
	private HeaderPrintWriter makeStream() {

		StringBuffer propName = new StringBuffer("derby.stream.error.");

		int prefixLength = propName.length();

		// get the header
		PrintWriterGetHeader header = makeHeader();

		// get the stream
		propName.setLength(prefixLength);
		
		HeaderPrintWriter hpw = makeHPW(propName, header);

		// If hpw == null then no properties were specified for the stream
		// so use/create the default stream.
		if (hpw == null)
			hpw = createDefaultStream(header);
		return hpw;
	}

	/**
		Return a new header object.
	*/
	private PrintWriterGetHeader makeHeader() {

		return new BasicGetLogHeader(true, true, (String) null);
	}

	/**
		create a HeaderPrintWriter based on the header.
		Will still need to determine the target type.
	 */
	private HeaderPrintWriter makeHPW(StringBuffer propPrefix,
		PrintWriterGetHeader header) {

		// the type of target is based on which property is used
		// to set it. choices are file, method, field, stream

		int prefixLength = propPrefix.length();

		// looking for derby.stream.<name>.file=<path to file>
		propPrefix.append("file");
		String target = PropertyUtil.getSystemProperty(propPrefix.toString());
		if (target!=null)
			return makeFileHPW(target, header);

		// looking for derby.stream.<name>.method=<className>.<methodName>
		propPrefix.setLength(prefixLength);
		propPrefix.append("method");
		target = PropertyUtil.getSystemProperty(propPrefix.toString());
		if (target!=null) 
			return makeMethodHPW(target, header);

		// looking for derby.stream.<name>.field=<className>.<fieldName>
		propPrefix.setLength(prefixLength);
		propPrefix.append("field");
		target = PropertyUtil.getSystemProperty(propPrefix.toString());
		if (target!=null) 
			return makeFieldHPW(target, header);

		return null;
	}

	/**
		Make a header print writer out of a file name. If it is a relative
		path name then it is taken as relative to derby.system.home if that is set,
		otherwise relative to the current directory. If the path name is absolute
		then it is taken as absolute.
	*/
	private HeaderPrintWriter PBmakeFileHPW(String fileName,
											PrintWriterGetHeader header) {

		boolean appendInfoLog = PropertyUtil.getSystemBoolean(Property.LOG_FILE_APPEND);

		File streamFile = new File(fileName);

		// See if this needs to be made relative to something ...
		if (!streamFile.isAbsolute()) {
			Object monitorEnv = Monitor.getMonitor().getEnvironment();
			if (monitorEnv instanceof File)
				streamFile = new File((File) monitorEnv, fileName);
		}

		FileOutputStream	fos;

		try {

			if (streamFile.exists() && appendInfoLog)
				fos = new FileOutputStream(streamFile.getPath(), true);
			else
				fos = new FileOutputStream(streamFile);
		} catch (IOException ioe) {
			return useDefaultStream(header, ioe);
		} catch (SecurityException se) {
			return useDefaultStream(header, se);
		}

		return new BasicHeaderPrintWriter(new BufferedOutputStream(fos), header, true);
	}

	private HeaderPrintWriter makeMethodHPW(String methodInvocation,
											PrintWriterGetHeader header) {

		int lastDot = methodInvocation.lastIndexOf('.');
		String className = methodInvocation.substring(0, lastDot);
		String methodName = methodInvocation.substring(lastDot+1);

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Method theMethod = theClass.getMethod(methodName,  new Class[0]);

				if (!Modifier.isStatic(theMethod.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theMethod.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theMethod, theMethod.invoke((Object) null, new Object[0]), header);
				} catch (IllegalAccessException iae) {
					t = iae;
				} catch (IllegalArgumentException iarge) {
					t = iarge;
				} catch (InvocationTargetException ite) {
					t = ite.getTargetException();
				}

			} catch (NoSuchMethodException nsme) {
				t = nsme;
			}
		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (SecurityException se) {
			t = se;
			
		}
		return useDefaultStream(header, t);

	}


	private HeaderPrintWriter makeFieldHPW(String fieldAccess,
											PrintWriterGetHeader header) {

		int lastDot = fieldAccess.lastIndexOf('.');
		String className = fieldAccess.substring(0, lastDot);
		String fieldName = fieldAccess.substring(lastDot+1,
							  fieldAccess.length());

		Throwable t;
		try {
			Class theClass = Class.forName(className);

			try {
				Field theField = theClass.getField(fieldName);
		
				if (!Modifier.isStatic(theField.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theField.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theField, theField.get((Object) null), header);
				} catch (IllegalAccessException iae) {
					t = iae;
				} catch (IllegalArgumentException iarge) {
					t = iarge;
				}

			} catch (NoSuchFieldException nsfe) {
				t = nsfe;
			}
		} catch (ClassNotFoundException cnfe) {
			t = cnfe;
		} catch (SecurityException se) {
			t = se;
		}
		return useDefaultStream(header, t);

		/*
			If we decide it is a bad idea to use reflect and need
			an alternate implementation, we can hard-wire those
			fields that we desire to give configurations access to,
			like so:

		if ("java.lang.System.out".equals(fieldAccess))
		 	os = System.out;
		else if ("java.lang.System.err".equals(fieldAccess))
		 	os = System.err;
		*/
	}

	private HeaderPrintWriter makeValueHPW(Member whereFrom, Object value,
		PrintWriterGetHeader header) {

		if (value instanceof OutputStream)
			 return new BasicHeaderPrintWriter((OutputStream) value, header, false);
		else if (value instanceof Writer)
			 return new BasicHeaderPrintWriter((Writer) value, header, false);
		
		HeaderPrintWriter hpw = useDefaultStream(header);

		if (value == null)
			hpw.printlnWithHeader(whereFrom.toString() + "=null");
		else
			hpw.printlnWithHeader(whereFrom.toString() + " instanceof " + value.getClass().getName());

		return hpw;
	}
 

	/**
		Used when no configuration information exists for a stream.
	*/
	private HeaderPrintWriter createDefaultStream(PrintWriterGetHeader header) {
		return makeFileHPW("derby.log", header);
	}

	/**
		Used when creating a stream creates an error.
	*/
	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header) {

		return new BasicHeaderPrintWriter(System.err, header, false);
	}

	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header, Throwable t) {

		HeaderPrintWriter hpw = useDefaultStream(header);
		hpw.printlnWithHeader(t.toString());
		return hpw;
	}

	/*
	** Priv block code, moved out of the old Java2 version.
	*/

    private String PBfileName;
    private PrintWriterGetHeader PBheader;

	private HeaderPrintWriter makeFileHPW(String fileName, PrintWriterGetHeader header)
    {
        this.PBfileName = fileName;
        this.PBheader = header;
        return (HeaderPrintWriter) java.security.AccessController.doPrivileged(this);
    }


    public final Object run()
    {
        // SECURITY PERMISSION - OP4, OP5
        return PBmakeFileHPW(PBfileName, PBheader);
    }
}

