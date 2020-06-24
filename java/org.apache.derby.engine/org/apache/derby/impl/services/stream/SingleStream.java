/*

   Derby - Class org.apache.derby.impl.services.stream.SingleStream

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

package org.apache.derby.impl.services.stream;

import org.apache.derby.shared.common.stream.InfoStreams;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.shared.common.stream.PrintWriterGetHeader;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.Writer;

import java.security.AccessController;
import java.security.PrivilegedAction;

import java.util.Properties;

import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Member;
import java.lang.reflect.InvocationTargetException;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.shared.common.reference.MessageId;

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
 */
public final class SingleStream
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
implements InfoStreams, ModuleControl, java.security.PrivilegedAction<HeaderPrintWriter>
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
	 * @see org.apache.derby.shared.common.stream.InfoStreams#stream
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

		// get the header
		PrintWriterGetHeader header = makeHeader();
		HeaderPrintWriter hpw = makeHPW(header);
//IC see: https://issues.apache.org/jira/browse/DERBY-205

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
	private HeaderPrintWriter makeHPW(PrintWriterGetHeader header) {

		// the type of target is based on which property is used
		// to set it. choices are file, method, field, stream

//IC see: https://issues.apache.org/jira/browse/DERBY-205
		String target = PropertyUtil.
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
		   getSystemProperty(Property.ERRORLOG_STYLE_PROPERTY);
		if (target != null) {
			return makeStyleHPW(target, header);
		}

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FILE_PROPERTY);
		if (target!=null)
			return makeFileHPW(target, header);

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_METHOD_PROPERTY);
		if (target!=null) 
			return makeMethodHPW(target, header, false);
//IC see: https://issues.apache.org/jira/browse/DERBY-6350

		target = PropertyUtil.
                   getSystemProperty(Property.ERRORLOG_FIELD_PROPERTY);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			Object monitorEnv = getMonitor().getEnvironment();
			if (monitorEnv instanceof File)
				streamFile = new File((File) monitorEnv, fileName);
		}

		FileOutputStream	fos;

		try {

			if (streamFile.exists() && appendInfoLog)
				fos = new FileOutputStream(streamFile.getPath(), true);
			else
				fos = new FileOutputStream(streamFile);
//IC see: https://issues.apache.org/jira/browse/DERBY-5363
            FileUtil.limitAccessToOwner(streamFile);
		} catch (IOException ioe) {
			return useDefaultStream(header, ioe);
		} catch (SecurityException se) {
			return useDefaultStream(header, se);
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-205
		return new BasicHeaderPrintWriter(new BufferedOutputStream(fos), header,
			true, streamFile.getPath());
	}

	private HeaderPrintWriter makeMethodHPW(String methodInvocation,
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
											PrintWriterGetHeader header,
                                            boolean canClose) {

		int lastDot = methodInvocation.lastIndexOf('.');
		String className = methodInvocation.substring(0, lastDot);
		String methodName = methodInvocation.substring(lastDot+1);

		Throwable t;
		try {
			Class<?> theClass = Class.forName(className);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

			try {
				Method theMethod = theClass.getMethod(methodName,  new Class[0]);

				if (!Modifier.isStatic(theMethod.getModifiers())) {
					HeaderPrintWriter hpw = useDefaultStream(header);
					hpw.printlnWithHeader(theMethod.toString() + " is not static");
					return hpw;
				}

				try {
					return makeValueHPW(theMethod, theMethod.invoke((Object) null, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
						new Object[0]), header, methodInvocation, canClose);
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

	private HeaderPrintWriter makeStyleHPW(String style,
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
											PrintWriterGetHeader header) {
		HeaderPrintWriter res = null;
		if ("rollingFile".equals(style)) {
		String className = "org.apache.derby.impl.services.stream.RollingFileStreamProvider.getOutputStream";
			res = makeMethodHPW(className, header, true);
		} else {            
			try {
				IllegalArgumentException ex = new IllegalArgumentException("unknown derby.stream.error.style: " + style);
                throw ex;
			} catch (IllegalArgumentException t) {
				res = useDefaultStream(header, t);
			} catch (Exception t) {
				res = useDefaultStream(header, t);
			}
		}
		return res;
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
					return makeValueHPW(theField, theField.get((Object) null), 
//IC see: https://issues.apache.org/jira/browse/DERBY-6350
						header, fieldAccess, false);
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
		PrintWriterGetHeader header, String name, boolean canClose) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6350

		if (value instanceof OutputStream)
			 return new BasicHeaderPrintWriter((OutputStream) value, header, canClose, name);
		else if (value instanceof Writer)
			 return new BasicHeaderPrintWriter((Writer) value, header, canClose, name);
		
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

//IC see: https://issues.apache.org/jira/browse/DERBY-205
		return new BasicHeaderPrintWriter(System.err, header, false, "System.err");
	}

	private HeaderPrintWriter useDefaultStream(PrintWriterGetHeader header, Throwable t) {

		HeaderPrintWriter hpw = useDefaultStream(header);

//IC see: https://issues.apache.org/jira/browse/DERBY-5363
        while (t != null) {
            Throwable causedBy = t.getCause();
            String causedByStr =
                MessageService.getTextMessage(MessageId.CAUSED_BY);
            hpw.printlnWithHeader(
                t.toString() + (causedBy != null ? " " + causedByStr : ""));
            t = causedBy;
        }

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


    public final HeaderPrintWriter run()
    {
        // SECURITY PERMISSION - OP4, OP5
        return PBmakeFileHPW(PBfileName, PBheader);
    }
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}

