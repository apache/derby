/*

   Derby - Class org.apache.derby.impl.sql.GenericStorablePreparedStatement

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

package org.apache.derby.impl.sql;

import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.util.ByteArray;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.Statement;
import org.apache.derby.iapi.sql.StorablePreparedStatement;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.monitor.Monitor;

import java.sql.Timestamp;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
/**
 * Prepared statement that can be made persistent.
 * @author jamie
 */
public class GenericStorablePreparedStatement
	extends GenericPreparedStatement implements Formatable, StorablePreparedStatement
{

	// formatable
	private ByteArray 		byteCode;
	private String 			className;

	/**
	 * Niladic constructor, for formatable
	 * only.
	 */
	public GenericStorablePreparedStatement()
	{
		super();
	}

	GenericStorablePreparedStatement(Statement stmt)
	{
		super(stmt);
	}

	/**
	 * Get our byte code array.  Used
	 * by others to save off our byte
	 * code for us.
	 *
	 * @return the byte code saver
	 */
	ByteArray getByteCodeSaver()
	{
		if (byteCode == null) {
			byteCode = new ByteArray();
		}
		return byteCode;
	}

	/**
	 * Get and load the activation class.  Will always
	 * return a loaded/valid class or null if the class
	 * cannot be loaded.  
	 *
	 * @return the generated class, or null if the
	 *		class cannot be loaded 
	 *
	 * @exception StandardException on error
	 */
	public GeneratedClass getActivationClass()
		throws StandardException
	{
		if (activationClass == null)
			loadGeneratedClass();

		return activationClass;
	}

	public void setActivationClass(GeneratedClass ac) {

		super.setActivationClass(ac);
		if (ac != null) {
			className = ac.getName();

			// see if this is an pre-compiled class
			if (byteCode != null && byteCode.getArray() == null)
				byteCode = null;
		}
	}

	/////////////////////////////////////////////////////////////
	// 
	// STORABLEPREPAREDSTATEMENT INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	/**
	 * Load up the class from the saved bytes.
	 *
	 * @return the generated class, or null if we
	 *	 don't have the byte code
	 *
	 * @exception StandardException on error
	 */
	public void loadGeneratedClass()
		throws StandardException
	{
		LanguageConnectionContext lcc =
			(LanguageConnectionContext) ContextService.getContext
				                                  (LanguageConnectionContext.CONTEXT_ID);
		ClassFactory classFactory = lcc.getLanguageConnectionFactory().getClassFactory();

		GeneratedClass gc = classFactory.loadGeneratedClass(className, byteCode);

		/*
		** No special try catch logic to write out bad classes
		** here.  We don't expect any problems, and in any 
		** event, we don't have the class builder available
		** here.
		*/
		setActivationClass(gc);
	}


	/////////////////////////////////////////////////////////////
	// 
	// EXTERNALIZABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeObject(getCursorInfo());
		out.writeBoolean(needsSavepoint());
		out.writeBoolean(isAtomic);
		out.writeObject(executionConstants);
		out.writeObject(resultDesc);

		// savedObjects may be null
		if (savedObjects == null)
		{
			out.writeBoolean(false);
		}
		else
		{	
			out.writeBoolean(true);
			ArrayUtil.writeArrayLength(out, savedObjects);
			ArrayUtil.writeArrayItems(out, savedObjects);
		}

		/*
		** Write out the class name and byte code
		** if we have them.  They might be null if
		** we don't want to write out the plan, and
		** would prefer it just write out null (e.g.
		** we know the plan is invalid).
		*/
		out.writeObject(className);
		out.writeBoolean(byteCode != null);
		if (byteCode != null)
		    byteCode.writeExternal(out);
	}

	 
	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 * @exception ClassNotFoundException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		setCursorInfo((CursorInfo)in.readObject());
		setNeedsSavepoint(in.readBoolean());
		isAtomic = (in.readBoolean());
		executionConstants = (ConstantAction) in.readObject();
		resultDesc = (ResultDescription) in.readObject();

		if (in.readBoolean())
		{
			savedObjects = new Object[ArrayUtil.readArrayLength(in)];
			ArrayUtil.readArrayItems(in, savedObjects);
		}

		className = (String)in.readObject();
		if (in.readBoolean()) {
			byteCode = new ByteArray();
			byteCode.readExternal(in);
		}
		else
			byteCode = null;
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int getTypeFormatId()	{ return StoredFormatIds.STORABLE_PREPARED_STATEMENT_V01_ID; }

	/////////////////////////////////////////////////////////////
	// 
	// MISC
	// 
	/////////////////////////////////////////////////////////////
	public boolean isStorable() {
		return true;
	}
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String acn;
			if (activationClass ==null)
				acn = "null";
			else
				acn = activationClass.getName();

 			return "GSPS " + System.identityHashCode(this) + " activationClassName="+acn+" className="+className;
		}
		else
		{
			return "";
		}
	} 
}
