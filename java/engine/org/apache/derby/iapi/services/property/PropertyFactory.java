/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.property
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.property;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.locks.LockFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;

import org.apache.derby.iapi.services.property.PropertySetCallback;
import java.util.Properties;
import java.io.File;
import java.io.Serializable;
import java.util.Dictionary;

/**
  Module interface for an Property validation.  

  <p>
  An PropertyFactory is typically obtained from the Monitor:
  <p>
  <blockquote><pre>
	// Get the current validation factory.
	PropertyFactory af;
	af = (PropertyFactory) Monitor.findServiceModule(this, org.apache.derby.iapi.reference.Module.PropertyFactory);
  </pre></blockquote>
**/

public interface PropertyFactory
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
    /**************************************************************************
     * methods that are Property related.
     **************************************************************************
     */

    /**
     * Add a callback for a change in any property value.
	 * <BR>
     * The callback is made in the context of the transaction making the change.
     *
     * @param who   which object is called
     **/
	public void addPropertySetNotification(
    PropertySetCallback     who);

    /**
     * Validate a Property set.
     * <p>
     * Validate a Property set by calling all the registered property set
     * notification functions with .
     *
	 * @param p Properties to validate.
	 * @param ignore Properties to not validate in p. Usefull for properties
	 *        that may not be set after boot. 
     *
	 * @exception StandardException Throws if p fails a check.
     **/
	public void verifyPropertySet(
    Properties p, 
    Properties ignore) 
        throws StandardException;

	/**
	 * validation a single property
	 */
	public void validateSingleProperty(String key,
						  Serializable value,
						  Dictionary set)
		throws StandardException;

	/**
	   
	 */
	public Serializable doValidateApplyAndMap(TransactionController tc,
											 String key, Serializable value,
											 Dictionary d, boolean dbOnlyProperty)
		throws StandardException;


	/**
	  Call the property set callbacks to map a proposed property value
	  to a value to save.
	  <P>
	  The caller must run this in a block synchronized on this
	  to serialize validations with changes to the set of
	  property callbacks
	  */
	public Serializable doMap(String key,
							 Serializable value,
							 Dictionary set)
		throws StandardException;
}
