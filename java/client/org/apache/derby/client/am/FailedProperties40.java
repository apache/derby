/*
 
   Derby - Class org.apache.derby.client.am.FailedProperties40
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.client.am;

import java.util.Properties;
import java.util.Enumeration;
import java.sql.ClientInfoException;

    /**
     * Class <code>FailedProperties40</code> is a helper class for
     * <code>java.sql.ClientInfoException</code>. It provides
     * convenient access to data that is needed when constructing
     * those exceptions. Should be kept in sync with its embedded
     * counter part.
     * @see java.sql.ClientInfoException
     * @see org.apache.derby.iapi.jdbc.FailedProperties40
     */
public class FailedProperties40 {
    private final Properties failedProps_ = new Properties();
    private final String firstKey_;
    private final String firstValue_;
    
    /**
     * Creates a new <code>FailedProperties40</code> instance. Since
     * Derby doesn't support any properties, all the keys from the
     * <code>props</code> parameter are added to the
     * <code>failedProps_</code> member with value
     * REASON_UNKNOWN_PROPERTY.
     *
     * @param props a <code>Properties</code> value. Can be null or empty
     */   
    public FailedProperties40(Properties props) {
        if (props == null || props.isEmpty()) {
            firstKey_ = null;
            firstValue_ = null;
            return;
        }
        Enumeration e = props.keys();
        firstKey_ = (String)e.nextElement();
        firstValue_ = props.getProperty(firstKey_);
        failedProps_.setProperty(firstKey_, ""+ClientInfoException.
                                 REASON_UNKNOWN_PROPERTY);
        while (e.hasMoreElements()) {
            failedProps_.setProperty((String)e.nextElement(), 
                                     ""+ClientInfoException.
                                     REASON_UNKNOWN_PROPERTY);
        }
    }

    /**
     * <code>getProperties</code> provides a <code>Properties</code>
     * object describing the failed properties (as specified in the
     * javadoc for java.sql.ClientInfoException).
     *
     * @return a <code>Properties</code> object with the failed
     * property keys and the reason why each failed
     */
    public Properties getProperties() { return failedProps_; }

    /**
     * <code>getFirstKey</code> returns the first property key. Used
     * when ClientInfoException is thrown with a parameterized error
     * message.
     *
     * @return a <code>String</code> value
     */
    public String getFirstKey() { return firstKey_; }

    /**
     * <code>getFirstValue</code> returns the first property value. Used
     * when ClientInfoException is thrown with a parameterized error
     * message.
     *
     * @return a <code>String</code> value
     */
    public String getFirstValue() { return firstValue_; }
}
