/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LongMagnitude

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

package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derby.agg.Aggregator;

/**
 * An aggregate which computes max(abs()), always returning a Long.
 */
public  class   LongMagnitude<V extends Number>    implements  Aggregator<V,Long,LongMagnitude<V>>
{
    private long    _magnitude;
    private boolean _null = true;

    public  LongMagnitude() {}

    public  void    init() {}
    public  void    accumulate( V value ) { more( value.longValue() ); }
    public  void    merge( LongMagnitude<V> other ) { more( other._magnitude ); }
    public  Long    terminate() { return _null ? null : _magnitude; }
    
    private void    more( long value )
    {
        value = value > 0 ? value : -value;
        _magnitude = _magnitude > value ? _magnitude : value;
        _null = false;
    }
    
}
