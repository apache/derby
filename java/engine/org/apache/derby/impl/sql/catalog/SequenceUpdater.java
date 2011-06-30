/*

   Derby - Class org.apache.derby.impl.sql.catalog.SequenceUpdater

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
package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.SequencePreallocator;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.SequenceDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;

/**
 * <p>
 * An object cached in the data dictionary which manages new values
 * for sequences. Note that this class must be public and
 * have a 0-arg constructor in order to satisfy the Cacheable contract.
 * </p>
 *
 * <p>
 * This is the abstract superclass of specific implementations for specific
 * sequences. For instance, one subclass handles the ANSI/ISO sequences
 * stored in SYSSEQUENCES. Another subclass could handle the sequences
 * stored in Derby's identity columns.
 * </p>
 *
 * <p>
 * This class does a couple tricky things:
 * </p>
 *
 * <ul>
 * <li>It pre-allocates a range of values from a sequence so that we don't have to change
 *  the on-disk value every time we get the next value for a sequence.</li>
 * <li>When updating the on-disk value, we first try to do the writing in
 *  a nested subtransaction. This is so that we can immediately release the write-lock afterwards.
 *  If that fails, we then try to do the writing in the user's execution transaction.</li>
 * </ul>
 *
 * <p>
 * Here is the algorithm pursued when the caller asks for the next number in a sequence:
 * </p>
 *
 *
 * <ul>
 * <li>We try to get the next number from a cache of pre-allocated numbers. The endpoint
 * (last number in the pre-allocated range) was previously recorded in the catalog row which
 * describes this sequence. If we are successful in getting the next number, we
 * return it and all is well.</li>
 * <li>Otherwise, we must allocate a new range by updating the catalog row. At this
 * point we may find ourselves racing another session, which also needs the next number
 * in the sequence.</li>
 * <li>When we try to update the catalog row, we check to see whether the current value
 * there is what we expect it to be. If it is, then all is well: we update the catalog row
 * then return to the first step to try to get the next number from the new cache of
 * pre-allocated numbers.</li>
 * <li>If, however, the value in the catalog row is not what we expect, then another
 * session has won the race to update the catalog. We accept this fact gracefully and
 * do not touch the catalog. Instead, we return to the first step and try to get the
 * next number from the new cache of numbers which the other session has just
 * pre-allocated.</li>
 * <li>We only allow ourselves to retry this loop a small number of times. If we still
 * can't get the next number in the sequence, we raise an exception complaining that
 * there is too much contention on the generator.</li>
 * </ul>
 *
 * <p>
 * If applications start seeing exceptions complaining that there is too much contention
 * on a sequence generator, then we should improve this algorithm. Here are some options
 * based on the idea that contention should go down if we increase the number of
 * pre-allocated numbers:
 * </p>
 *
 * <ul>
 * <li>We can let the user change the size of the pre-allocated range.</li>
 * <li>Derby can increase the size of the pre-allocated range when Derby detects
 * too much contention.</li>
 * </ul>
 *
 */
public abstract class SequenceUpdater implements Cacheable
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANT STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // DataDictionary where this generator is cached.
    protected DataDictionaryImpl _dd;

    // This is the key used to lookup this generator in the cache.
    protected String _uuidString;

    // This is the object which allocates ranges of sequence values
    protected SequenceGenerator _sequenceGenerator;

    // This is the lock timeout in milliseconds; a negative number means no timeout
    private long _lockTimeoutInMillis;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** No-arg constructor to satisfy the Cacheable contract */
    public SequenceUpdater()
    {
        _lockTimeoutInMillis = getLockTimeout();
    }

    /** Normal constructor */
    public SequenceUpdater( DataDictionaryImpl dd )
    {
        this();
        
        _dd = dd;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT BEHAVIOR TO BE IMPLEMENTED BY CHILDREN
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Initialize the sequence generator. Work is done inside a read-only subtransaction of
     * the session's execution transaction.
     * </p>
     */
     abstract protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
         throws StandardException;

    /**
     * <p>
     * Update the sequence value on disk. This method is first called with a read/write subtransaction
     * of the session's execution transaction. If work can't be done there immediately, this method
     * is called with the session's execution transaction.
     * </p>
     *
     * @param tc The transaction to use
     * @param oldValue Expected value on disk for this sequence
     * @param newValue The value to poke into the system table backing this sequence
     * @param wait Whether to wait for a lock
     *
	 * 
	 * @return Returns true if the value was successfully updated, false if we lost a race with another session.
     *
     * @throws StandardException May throw an exception if a lock can't be obtained.
     */
    abstract protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait ) throws StandardException;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Cacheable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void clean(boolean forRemove) throws StandardException
	{
        //
        // Flush current value to disk. This prevents us from leaking values when DDL
        // is performed. The metadata caches are invalidated and cleared when DDL is performed.
        //
        if ( _sequenceGenerator != null )
        {
            updateCurrentValueOnDisk( null, peekAtCurrentValue() );
        }

        _uuidString = null;
        _sequenceGenerator = null;
	}
    
	public boolean isDirty() { return false; }
    public Object getIdentity() { return _uuidString; }

	public void clearIdentity()
    {
        try
        {
            clean( false );
        } catch (StandardException se)
        {
            //Doing check for lcc and db to be certain
            LanguageConnectionContext lcc = getLCC();
            if (lcc != null)
            {
                Database db = lcc.getDatabase();
                boolean isactive = (db != null ? db.isActive() : false);
                lcc.getContextManager().cleanupOnError(se, isactive);
            }
        }
    }

	public Cacheable createIdentity( Object key, Object createParameter ) throws StandardException
	{
        Cacheable cacheable = this;

        //
        // The createParameter arg is unused.
        //
        return cacheable.setIdentity( key );
	}

	/**
	 * @see Cacheable#setIdentity
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Cacheable setIdentity(Object key) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (!(key instanceof String))
			{
				SanityManager.THROWASSERT( "Key for a SequenceUpdater is a " + key.getClass().getName() );
			}

            if ( (_uuidString != null) || (_sequenceGenerator != null) )
			{
				SanityManager.THROWASSERT( "Identity being changed on a live cacheable. Old uuidString = " + _uuidString );
			}
		}

		_uuidString = (String) key;

        if ( _sequenceGenerator == null )
        {
            TransactionController executionTC = getLCC().getTransactionExecute();
            
            //
            // We lookup information in a read-only subtransaction in order to minimize
            // contention. Since this is a read-only subtransaction, there should be
            // no conflict with the parent transaction.
            //
            TransactionController subTransaction = executionTC.startNestedUserTransaction( true );
            try {
                _sequenceGenerator = createSequenceGenerator( subTransaction );
            }
            finally
            {
                subTransaction.commit();
                subTransaction.destroy();
            }
        }

		if ( _sequenceGenerator != null ) { return this; }
		else { return null; }
	}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the next sequence number managed by this generator and advance the number. Could raise an
     * exception if the legal range is exhausted and wrap-around is not allowed.
     * Only one thread at a time is allowed through here. That synchronization is performed by
     * the sequence generator itself.
     * </p>
     *
     * @param returnValue This value is stuffed with the new sequence number.
     */
    public void getCurrentValueAndAdvance
        ( NumberDataValue returnValue ) throws StandardException
    {
        Long startTime = null;
        
        //
        // We try to get a sequence number. We try a couple times in case we find
        // ourselves in a race with another session which is draining numbers from
        // the same sequence generator.
        //
        while ( true )
        {
            long[] cvaa = _sequenceGenerator.getCurrentValueAndAdvance();
            
            int status = (int) cvaa[ SequenceGenerator.CVAA_STATUS ];
            long currentValue = cvaa[ SequenceGenerator.CVAA_CURRENT_VALUE ];
            long lastAllocatedValue = cvaa[ SequenceGenerator.CVAA_LAST_ALLOCATED_VALUE ];
            long numberOfValuesAllocated = cvaa[ SequenceGenerator.CVAA_NUMBER_OF_VALUES_ALLOCATED ];
            
            switch ( status )
            {
            case SequenceGenerator.RET_OK:
                returnValue.setValue( currentValue );
                return;
                
            case SequenceGenerator.RET_MARK_EXHAUSTED:
                updateCurrentValueOnDisk( new Long( currentValue ), null );
                returnValue.setValue( currentValue );
                return;
                
            case SequenceGenerator.RET_ALLOCATE_NEW_VALUES:
                
                if ( updateCurrentValueOnDisk( new Long( currentValue ), new Long( lastAllocatedValue ) ) )
                {
                    _sequenceGenerator.allocateNewRange( currentValue, numberOfValuesAllocated );
                }
                break;
                
            default:
                throw unimplementedFeature();
            }

            //
            // If we get here, then we failed to get a sequence number. Along the way,
            // we or another session may have allocated more sequence numbers on disk. We go back
            // in to try to grab one of those numbers.
            //
            if ( startTime == null )
            {
                // get the system time only if we have to
                startTime = new Long( System.currentTimeMillis() );
                continue;
            }
            
            if (
                (_lockTimeoutInMillis >= 0L) &&
                ( (System.currentTimeMillis() - startTime.longValue()) > _lockTimeoutInMillis )
                )
            {
                break;
            }
            
        } // end of retry loop

        //
        // If we get here, then we exhausted our retry attempts. This might be a sign
        // that we need to increase the number of sequence numbers which we
        // allocate. There's an opportunity for Derby to tune itself here.
        //
        throw StandardException.newException
            ( SQLState.LANG_TOO_MUCH_CONTENTION_ON_SEQUENCE, _sequenceGenerator.getName() );
    }

    /**
     * <p>
     * Get the current value of the sequence generator without advancing it.
     * May return null if the generator is exhausted.
     * </p>
     */
    private Long peekAtCurrentValue() throws StandardException
    {
        return _sequenceGenerator.peekAtCurrentValue();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // DISK WRITING MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Update the value on disk. First tries to update the value in a
     * subtransaction. If that fails, falls back on the execution transaction.
     * This is a callback method invoked by the sequence generator.
     * </p>
	 * 
	 * @return Returns true if the value was successfully updated, false if we lost a race with another session.
     *
     */
    public boolean updateCurrentValueOnDisk( Long oldValue, Long newValue ) throws StandardException
    {
        TransactionController executionTransaction = getLCC().getTransactionExecute();
        TransactionController nestedTransaction = null;

        try {
            nestedTransaction = executionTransaction.startNestedUserTransaction( false );
        } catch (StandardException se) {}
        
        // First try to do the work in the nested transaction. Fail if we can't
        // get a lock immediately.
        if ( nestedTransaction != null )
        {
            try {
                return updateCurrentValueOnDisk( nestedTransaction, oldValue, newValue, false );
            }
            catch (StandardException se)
            {
                if ( !se.getMessageId().equals( SQLState.LOCK_TIMEOUT ) ) { throw se; }
            }
            finally
            {
                nestedTransaction.commit();
                nestedTransaction.destroy();
            }
        }
        
        // If we get here, we failed to do the work in the nested transaction.
        // Fall back on the execution transaction
        
        return updateCurrentValueOnDisk( executionTransaction, oldValue, newValue, true );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // UTILITY MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Make a new range allocator (called when the generator is instantiated) */
    protected SequencePreallocator  makePreallocator( TransactionController tc )
        throws StandardException
    {
        String  propertyName = Property.LANG_SEQUENCE_PREALLOCATOR;
        String  className = PropertyUtil.getServiceProperty( tc, propertyName );

        if ( className == null ) { return new SequenceRange(); }

        try {
            // If the property value was a number rather than a class name, then
            // use that as the default size for preallocated ranges.
            if ( isNumber( className ) )
            {
                return new SequenceRange( Integer.parseInt( className ) );
            }
            
            return (SequencePreallocator) Class.forName( className ).newInstance();
        }
        catch (ClassNotFoundException e) { throw missingAllocator( propertyName, className, e ); }
        catch (ClassCastException e) { throw missingAllocator( propertyName, className, e ); }
        catch (InstantiationException e) { throw missingAllocator( propertyName, className, e ); }
        catch (IllegalAccessException e) { throw missingAllocator( propertyName, className, e ); }
        catch (NumberFormatException e) { throw missingAllocator( propertyName, className, e ); }
    }
    private StandardException   missingAllocator( String propertyName, String className, Exception e )
    {
        return StandardException.newException( SQLState.LANG_UNKNOWN_SEQUENCE_PREALLOCATOR, e, propertyName, className );
    }
    private boolean isNumber( String text )
    {
        int length = text.length();

        for ( int i = 0; i < length; i++ )
        {
            if ( !Character.isDigit( text.charAt( i ) ) ) { return false; }
        }

        return true;
    }
    
    /** Get the time we wait for a lock, in milliseconds--overridden by unit tests */
    protected int getLockTimeout()
    {
        return getLCC().getTransactionExecute().getAccessManager().getLockFactory().getWaitTimeout();
    }
    
	private static LanguageConnectionContext getLCC()
    {
		return (LanguageConnectionContext) 
					ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
	}

    /** Report an unimplemented feature */
    private StandardException unimplementedFeature()
    {
        return StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Specific implementation of SequenceUpdater for the sequences managed by
     * SYSCOLUMNS.
     * </p>
     */
    public static final class SyscolumnsUpdater extends SequenceUpdater
    {
        private RowLocation _sequenceRowLocation;

        public SyscolumnsUpdater() { super(); }
        public SyscolumnsUpdater( DataDictionaryImpl dd ) { super( dd ); }
    
        //
        // SequenceUpdater BEHAVIOR
        //

        protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
            throws StandardException
        {
            RowLocation[] rowLocation = new RowLocation[ 1 ];
            SequenceDescriptor[] sequenceDescriptor = new SequenceDescriptor[ 1 ];
            
            _dd.computeIdentityRowLocation( readOnlyTC, _uuidString, rowLocation, sequenceDescriptor );
            
            _sequenceRowLocation = rowLocation[ 0 ];
            
            SequenceDescriptor isd = sequenceDescriptor[ 0 ];
            
            return new SequenceGenerator
                (
                 isd.getCurrentValue(),
                 isd.canCycle(),
                 isd.getIncrement(),
                 isd.getMaximumValue(),
                 isd.getMinimumValue(),
                 isd.getStartValue(),
                 isd.getSchemaDescriptor().getSchemaName(),
                 isd.getSequenceName(),
                 makePreallocator( readOnlyTC )
                 );
        }

        protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait ) throws StandardException
        {
            return _dd.updateCurrentIdentityValue( tc, _sequenceRowLocation, wait, oldValue, newValue );
        }
    }

    /**
     * <p>
     * Specific implementation of SequenceUpdater for the sequences managed by
     * SYSSEQUENCES.
     * </p>
     */
    public static final class SyssequenceUpdater extends SequenceUpdater
    {
        private RowLocation _sequenceRowLocation;

        public SyssequenceUpdater() { super(); }
        public SyssequenceUpdater( DataDictionaryImpl dd ) { super( dd ); }
    
        //
        // SequenceUpdater BEHAVIOR
        //

        protected SequenceGenerator createSequenceGenerator( TransactionController readOnlyTC )
            throws StandardException
        {
            RowLocation[] rowLocation = new RowLocation[ 1 ];
            SequenceDescriptor[] sequenceDescriptor = new SequenceDescriptor[ 1 ];
            
            _dd.computeSequenceRowLocation( readOnlyTC, _uuidString, rowLocation, sequenceDescriptor );
            
            _sequenceRowLocation = rowLocation[ 0 ];
            
            SequenceDescriptor isd = sequenceDescriptor[ 0 ];
            
            return new SequenceGenerator
                (
                 isd.getCurrentValue(),
                 isd.canCycle(),
                 isd.getIncrement(),
                 isd.getMaximumValue(),
                 isd.getMinimumValue(),
                 isd.getStartValue(),
                 isd.getSchemaDescriptor().getSchemaName(),
                 isd.getSequenceName(),
                 makePreallocator( readOnlyTC )
                 );
        }

        protected boolean updateCurrentValueOnDisk( TransactionController tc, Long oldValue, Long newValue, boolean wait ) throws StandardException
        {
            return _dd.updateCurrentSequenceValue( tc, _sequenceRowLocation, wait, oldValue, newValue );
        }
    }

}

