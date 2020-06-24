/*

   Derby - Class org.apache.derby.impl.sql.compile.TypeCompilerFactoryImpl

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

package org.apache.derby.impl.sql.compile;

import java.sql.Types;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.sql.compile.TypeCompilerFactory;
import org.apache.derby.iapi.types.TypeId;

public class TypeCompilerFactoryImpl implements TypeCompilerFactory
{
        private static final String PACKAGE_NAME =
                        "org.apache.derby.impl.sql.compile.";

        // These are all the TypeCompilers that are stateless, so we can
        // use a single instance of each. Initialize all to null, and fault
        // them in.
        static TypeCompiler bitTypeCompiler;
        static TypeCompiler booleanTypeCompiler;
        static TypeCompiler charTypeCompiler;
        static TypeCompiler decimalTypeCompiler ;
        static TypeCompiler doubleTypeCompiler ;
        static TypeCompiler intTypeCompiler ;
        static TypeCompiler longintTypeCompiler ;
        static TypeCompiler longvarbitTypeCompiler ;
        static TypeCompiler longvarcharTypeCompiler ;
        static TypeCompiler realTypeCompiler ;
        static TypeCompiler smallintTypeCompiler ;
        static TypeCompiler tinyintTypeCompiler ;
        static TypeCompiler dateTypeCompiler ;
        static TypeCompiler timeTypeCompiler ;
        static TypeCompiler timestampTypeCompiler ;
        static TypeCompiler varbitTypeCompiler ;
        static TypeCompiler varcharTypeCompiler ;
        static TypeCompiler refTypeCompiler ;
        static TypeCompiler blobTypeCompiler ;
        static TypeCompiler clobTypeCompiler ;
        static TypeCompiler xmlTypeCompiler ;

        /**
         * Get a TypeCompiler corresponding to the given TypeId
         *
         * @param typeId        The TypeId to get a TypeCompiler for
         *
         * @return      The corresponding TypeCompiler
         */

        public TypeCompiler getTypeCompiler(TypeId typeId)
        {
                return staticGetTypeCompiler(typeId);
        }

        static TypeCompiler staticGetTypeCompiler(TypeId typeId)
        {
                String sqlTypeName;

                switch (typeId.getJDBCTypeId())
                {
                  case Types.BINARY:
                        return bitTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                                        bitTypeCompiler,
                                                                        typeId);

                  case Types.BIT:
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
                  case Types.BOOLEAN:
                        return booleanTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "BooleanTypeCompiler",
                                                                booleanTypeCompiler,
                                                                typeId);

                  case Types.CHAR:
                          sqlTypeName = typeId.getSQLTypeName();
//IC see: https://issues.apache.org/jira/browse/DERBY-2720
//IC see: https://issues.apache.org/jira/browse/DERBY-3315
                          return charTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      charTypeCompiler,
                                                      typeId);

                  case Types.NUMERIC:
                  case Types.DECIMAL:
                        return decimalTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                decimalTypeCompiler,
                                                                typeId);

                  case Types.DOUBLE:
                        return doubleTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                doubleTypeCompiler,
                                                                typeId);

                  case Types.INTEGER:
                        return intTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                intTypeCompiler,
                                                                typeId);

                  case Types.BIGINT:
                        return longintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                longintTypeCompiler,
                                                                typeId);

//IC see: https://issues.apache.org/jira/browse/DERBY-3484
                  case Types.BLOB:
                        return blobTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "LOBTypeCompiler",
                                                          blobTypeCompiler,
                                                          typeId);

                  case Types.LONGVARBINARY:
                        return longvarbitTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                          longvarbitTypeCompiler,
                                                          typeId);

//IC see: https://issues.apache.org/jira/browse/DERBY-3484
                  case Types.CLOB:
                      sqlTypeName = typeId.getSQLTypeName();
//IC see: https://issues.apache.org/jira/browse/DERBY-2720
//IC see: https://issues.apache.org/jira/browse/DERBY-3315
                      return clobTypeCompiler =
                          getAnInstance(PACKAGE_NAME + "CLOBTypeCompiler",
                                        clobTypeCompiler,
                                        typeId);
                  case Types.LONGVARCHAR:
                          sqlTypeName = typeId.getSQLTypeName();
                          return longvarcharTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      longvarcharTypeCompiler,
                                                      typeId);

                  case Types.REAL:
                        return realTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                realTypeCompiler,
                                                                typeId);

                  case Types.SMALLINT:
                        return smallintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                smallintTypeCompiler,
                                                                typeId);

                  case Types.TINYINT:
                    return tinyintTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "NumericTypeCompiler",
                                                                tinyintTypeCompiler,
                                                                typeId);

                  case Types.DATE:
                        return dateTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "DateTypeCompiler",
                                                                        dateTypeCompiler,
                                                                        typeId);

                  case Types.TIME:
                        return timeTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "TimeTypeCompiler",
                                                                        timeTypeCompiler,
                                                                        typeId);
                  case Types.TIMESTAMP:
                        return timestampTypeCompiler =
                                        getAnInstance(PACKAGE_NAME + "TimestampTypeCompiler",
                                                                        timestampTypeCompiler,
                                                                        typeId);
                  case Types.VARBINARY:
                        return varbitTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "BitTypeCompiler",
                                                                varbitTypeCompiler,
                                                                typeId);

                  case Types.VARCHAR:
                          sqlTypeName = typeId.getSQLTypeName();
//IC see: https://issues.apache.org/jira/browse/DERBY-2720
//IC see: https://issues.apache.org/jira/browse/DERBY-3315
                          return varcharTypeCompiler =
                              getAnInstance(PACKAGE_NAME + "CharTypeCompiler",
                                                      varcharTypeCompiler,
                                                      typeId);

//IC see: https://issues.apache.org/jira/browse/DERBY-3484
                  case Types.JAVA_OBJECT:
                  case Types.OTHER:
                        if (typeId.isRefTypeId())
                        {
                                return refTypeCompiler = getAnInstance(
                                                                                        PACKAGE_NAME + "RefTypeCompiler",
                                                                                        refTypeCompiler,
                                                                                        typeId);
                        }
                        else
                        {
                                // Cannot re-use instances of user-defined type compilers,
                                // because they contain the class name
                                BaseTypeCompiler btc = new UserDefinedTypeCompiler();
                                btc.setTypeId(typeId);
                                return btc;
                        }

//IC see: https://issues.apache.org/jira/browse/DERBY-2438
                  case Types.SQLXML:
                        return xmlTypeCompiler =
                                getAnInstance(PACKAGE_NAME + "XMLTypeCompiler",
                                                                xmlTypeCompiler,
                                                                typeId);

                }

                if (SanityManager.DEBUG)
                {
                        SanityManager.THROWASSERT("Unexpected JDBC type id " +
                                                                                typeId.getJDBCTypeId() +
                                                                                " for typeId of class " +
                                                                                typeId.getClass().getName());
                }

                return null;
        }

        /**
         * Check whether the given TypeCompiler has been allocated yet.
         * If so, just return it, otherwise allocate a new instance
         * given its class.
         */
        private static TypeCompiler getAnInstance(String className,
                                                                TypeCompiler anInstance,
                                                                TypeId typeId)
        {
                if (anInstance == null)
                {
                        Exception exc = null;
                        Class<?> typeCompilerClass = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-673

                        try
                        {
                                typeCompilerClass = Class.forName(className);
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                                anInstance  = (TypeCompiler)
                                    typeCompilerClass.getConstructor().newInstance();
                                ((BaseTypeCompiler) anInstance).setTypeId(typeId);
                        }
                        catch (ClassNotFoundException cnfe)
                        {
                                exc = cnfe;
                        }
                        catch (IllegalAccessException iae)
                        {
                                exc = iae;
                        }
                        catch (InstantiationException ie)
                        {
                                exc = ie;
                        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                        catch (NoSuchMethodException nsme)
                        {
                                exc = nsme;
                        }
                        catch (java.lang.reflect.InvocationTargetException ite)
                        {
                                exc = ite;
                        }

                        if (SanityManager.DEBUG)
                        {
                                if (exc != null)
                                {
                                        SanityManager.THROWASSERT(
                                                "Exception " +
                                                exc +
                                                " while trying to get new instance of a " +
                                                typeCompilerClass.getName());
                                }
                        }
                }

                return anInstance;
        }
}
