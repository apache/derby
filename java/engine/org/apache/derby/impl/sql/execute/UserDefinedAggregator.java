/*

   Derby - Class org.apache.derby.impl.sql.execute.UserDefinedAggregator

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

package org.apache.derby.impl.sql.execute;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.derby.agg.Aggregator;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;

/**
	Aggregator for user-defined aggregates. Wraps the application-supplied
    implementation of org.apache.derby.agg.Aggregator.
 */
public final class UserDefinedAggregator  implements ExecAggregator
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final int FIRST_VERSION = 0;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private Aggregator  _aggregator;
    private DataTypeDescriptor  _resultType;
    private boolean     _eliminatedNulls;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor for Formatable interface */
    public  UserDefinedAggregator() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ExecAggregator BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void setup( ClassFactory classFactory, String aggregateName, DataTypeDescriptor resultType )
	{
        try {
            setup( classFactory.loadApplicationClass( aggregateName ), resultType );
        }
        catch (ClassNotFoundException cnfe) { logAggregatorInstantiationError( aggregateName, cnfe ); }
	}
    /** Initialization logic shared by setup() and newAggregator() */
    private void    setup( Class<?> udaClass, DataTypeDescriptor resultType )
    {
        String  aggregateName = udaClass.getName();
        
        try {
            _aggregator = (Aggregator) udaClass.getConstructor().newInstance();
            _aggregator.init();
        }
        catch (InstantiationException ie) { logAggregatorInstantiationError( aggregateName, ie ); }
        catch (IllegalAccessException iae) { logAggregatorInstantiationError( aggregateName, iae ); }
        catch (NoSuchMethodException nsme) { logAggregatorInstantiationError( aggregateName, nsme ); }
        catch (java.lang.reflect.InvocationTargetException ite) { logAggregatorInstantiationError( aggregateName, ite ); }

        _resultType = resultType;
    }

	public boolean didEliminateNulls() { return _eliminatedNulls; }

    @SuppressWarnings("unchecked")
	public void accumulate( DataValueDescriptor addend, Object ga ) 
		throws StandardException
	{
		if ( (addend == null) || addend.isNull() )
        {
			_eliminatedNulls = true;
			return;
		}

        Object  value = addend.getObject();

        _aggregator.accumulate( value );
	}

    @SuppressWarnings("unchecked")
	public void merge(ExecAggregator addend)
		throws StandardException
	{
        UserDefinedAggregator  other = (UserDefinedAggregator) addend;

        _aggregator.merge( other._aggregator );
	}

	/**
	 * Return the result of the aggregation. .
	 *
	 * @return the aggregated result (could be a Java null).
	 */
	public DataValueDescriptor getResult() throws StandardException
	{
        Object  javaReturnValue = _aggregator.terminate();

        if ( javaReturnValue == null ) { return null; }

        DataValueDescriptor dvd = _resultType.getNull();
        dvd.setObjectForCast( javaReturnValue, true, javaReturnValue.getClass().getName() );

        return dvd;
	}

	/**
	 */
	public ExecAggregator newAggregator()
	{
		UserDefinedAggregator   uda = new UserDefinedAggregator();

        uda.setup( _aggregator.getClass(), _resultType );

        return uda;
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt( FIRST_VERSION );
        
        out.writeObject( _aggregator );
        out.writeObject( _resultType );
        out.writeBoolean( _eliminatedNulls );
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		in.readInt();   // unused until we have a second rev of this class

        _aggregator = (Aggregator) in.readObject();
        _resultType = (DataTypeDescriptor) in.readObject();
        _eliminatedNulls = in.readBoolean();
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_USER_ADAPTOR_V01_ID; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Record an instantiation error trying to load the aggregator class.
     */
    private void   logAggregatorInstantiationError( String aggregateName, Throwable t )
    {
        String  errorMessage = MessageService.getTextMessage
            (
             MessageId.CM_CANNOT_LOAD_CLASS,
             aggregateName,
             t.getMessage()
             );

		Monitor.getStream().println( errorMessage );

        Exception   e = new Exception( errorMessage, t );

        e.printStackTrace( Monitor.getStream().getPrintWriter() );
    }

}
