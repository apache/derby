/*

   Derby - Class org.apache.derby.iapi.sql.depend.DependableList

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

package org.apache.derby.iapi.sql.depend;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.StreamCorruptedException;

import java.util.Vector;

/**
  A serializable list of Dependables.

  @author	Rick

 */
public class DependableList implements Formatable
{

	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	private	transient	boolean		ignoreAdds;

	Vector		uuids = null;
	Vector		dependableFinders;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////


	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 */
    public	DependableList()
	{
		uuids = new Vector();
		dependableFinders = new Vector();
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	PUBLIC INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Toggle whether new elements can be added to this list.
	  *
	  *	@param	newState	true if adds are to be ignored
	  *						false if adds are to be honored
	  */
    public	void	ignoreAdds( boolean newState ) { ignoreAdds = newState; }

	/**
	  *	Report whether we're ignoring adds
	  *
	  *	@return	true if we're ignoring adds
	  */
	public	boolean	ignoringAdds() { return ignoreAdds; }

	/**
	  *	Add another Dependable to the list.
	  *
	  *	@param	dependable	the next dependable to put on the list
	  */
	public	void	addDependable( Dependable dependable )
	{
		if ( ignoreAdds ) { return; }

		uuids.addElement( dependable.getObjectID() );
		dependableFinders.addElement( dependable.getDependableFinder() );
	}

	/**
	  *	Gets the number of Dependables in this list.
	  *
	  *	@return	the number of Dependables in this list.
	  */
    public	int		size() { return uuids.size(); }

	/**
	  *	Get the Dependable at this index.
	  *
	  *	@param	index	0-based index into this list
	  *
	  *	@return	the Dependable at this index
	  *
	  * @exception StandardException thrown if something goes wrong
	  */
	public	Dependable	dependableAt( int index )
		throws StandardException
	{
		UUID				id = (UUID) uuids.elementAt( index );
		DependableFinder	df = (DependableFinder) dependableFinders.elementAt( index );

		Dependable			dependable;
		try {
			dependable = df.getDependable( id );
		} catch (java.sql.SQLException te) {
			throw StandardException.newException(SQLState.DEP_UNABLE_TO_RESTORE, df.getClass().getName(), te.getMessage());
		}

		return	dependable;
	}

	///////////////////////////////////////////////////////////////////////
	//
	//	FORMATABLE INTERFACE METHODS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.DEPENDABLE_LIST_ID; }

	/**
	  @see java.io.Externalizable#readExternal
	  @exception IOException thrown on error
	  @exception ClassNotFoundException	thrown on error
	  */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		int					count = in.readInt();

		for ( int ictr = 0; ictr < count; ictr++ )
		{
			uuids.addElement( in.readObject() );
			dependableFinders.addElement( in.readObject() );
		}

	}

	/**

	  @exception IOException thrown on error
	  */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		int					count = uuids.size();

		out.writeInt( count );

		for ( int ictr = 0; ictr < count; ictr++ )
		{
			out.writeObject( uuids.elementAt( ictr ) );
			out.writeObject( dependableFinders.elementAt( ictr ) );
		}


	}

}
