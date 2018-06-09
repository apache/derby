/*

   Derby - Class org.apache.derby.iapi.services.cache.ClassSizeCatalog

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

package org.apache.derby.iapi.services.cache;

import java.util.Hashtable;

/**
 * Map from class names to size coefficients. The size coefficients can be
 * used to estimate how much memory an instance of the class takes.
 * @see ClassSize#getSizeCoefficients(Class)
 */
abstract class ClassSizeCatalog extends Hashtable<String, int[]> {
    /** The singleton instance of this class. */
    private static final ClassSizeCatalog INSTANCE;
    static {
        // Do not let the compiler see ClassSizeCatalogImpl. Otherwise it will
        // try to compile it. This may fail because ClassSizeCatalogImpl.java
        // is not created until everything else has been compiled. Bury
        // ClassSizeCatalogImpl in a string.
        String className = ClassSizeCatalog.class.getName() + "Impl";
        try {
            Class<?> clazz = Class.forName(className);
            INSTANCE = (ClassSizeCatalog) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            System.out.println("Got error while instantiating " + className + ": " + e.getMessage());
            e.printStackTrace();
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Get the singleton {@code ClassSizeCatalog} instance.
     */
    static ClassSizeCatalog getInstance() {
        return INSTANCE;
    }
}
