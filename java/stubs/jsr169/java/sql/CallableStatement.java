/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package java.sql;

import java.util.Calendar;
import java.util.Map;
import java.net.URL;
import java.io.InputStream;
import java.io.Reader;

/**
 * An interface used to call Stored Procedures.
 * <p>
 * The JDBC API provides an SQL escape syntax allowing Stored Procedures to be
 * called in a standard way for all databases. The JDBC escape syntax has two
 * forms. One form includes a result parameter. The second form does not include
 * a result parameter. Where the result parameter is used, it must be declared
 * as an OUT parameter. Other parameters can be declared as IN, OUT or INOUT.
 * Parameters are referenced either by name or by a numerical index, with the
 * first parameter being 1, the second 1 and so on. Here are examples of the two
 * forms of the escape syntax: <code>
 * 
 * { ?= call &lt.procedurename&gt.[([parameter1,parameter2,...])]}
 * 
 * {call &lt.procedurename&gt.[([parameter1,parameter2,...])]}
 * </code>
 * <p>
 * IN parameters are set before calling the procedure, using the setter methods
 * which are inherited from <code>PreparedStatement</code>. For OUT
 * parameters, their Type must be registered before executing the stored
 * procedure, and the value is retrieved using the getter methods defined in the
 * CallableStatement interface.
 * <p>
 * CallableStatements can return one or more ResultSets. Where multiple
 * ResultSets are returned they are accessed using the methods inherited from
 * the <code>Statement</code> interface.
 */
public interface CallableStatement extends PreparedStatement {

    /**
     * Gets the value of a specified JDBC BLOB parameter as a java.sql.Blob
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a java.sql.Blob with the value. null if the value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Blob getBlob(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC BIT parameter as a boolean
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a boolean representing the parameter value. false if the value is
     *         SQL NULL
     * @throws SQLException
     *             if a database error happens
     */
    public boolean getBoolean(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC TINYINT parameter as a byte
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a byte with the value of the parameter. 0 if the value is SQL
     *         NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public byte getByte(int parameterIndex) throws SQLException;

    /**
     * Returns a byte array representation of the indexed JDBC
     * <code>BINARY</code> or <code>VARBINARY</code> parameter.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return an array of bytes with the value of the parameter. null if the
     *         value is SQL NULL.
     * @throws SQLException
     *             if there is a problem accessing the database
     */
    public byte[] getBytes(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC CLOB parameter as a java.sql.Clob
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a java.sql.Clob with the value of the parameter. null if the
     *         value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Clob getClob(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC DATE parameter as a java.sql.Date.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return the java.sql.Date with the parameter value. null if the value is
     *         SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Date getDate(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC DATE parameter as a java.sql.Date.,
     * using a specified Calendar to construct the date.
     * <p>
     * The JDBC driver uses the Calendar to create the Date using a particular
     * timezone and locale. Default behaviour of the driver is to use the Java
     * virtual machine default settings.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param cal
     *            the Calendar to use to construct the Date
     * @return the java.sql.Date with the parameter value. null if the value is
     *         SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException;

    /**
     * Gets the value of a specified JDBC DOUBLE parameter as a double
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return the double with the parameter value. 0.0 if the value is SQL
     *         NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public double getDouble(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC FLOAT parameter as a float
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return the float with the parameter value. 0.0 if the value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public float getFloat(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC INTEGER parameter as an int
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return the int with the parameter value. 0 if the value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public int getInt(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC BIGINT parameter as a long
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return the long with the parameter value. 0 if the value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public long getLong(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified parameter as a Java <code>Object</code>.
     * <p>
     * The object type returned is the JDBC type registered for the parameter
     * with a <code>registerOutParameter</code> call. If a parameter was
     * registered as a <code>java.sql.Types.OTHER</code> then it may hold
     * abstract types that are particular to the connected database.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return an Object holding the value of the parameter.
     * @throws SQLException
     *             if there is a problem accessing the database
     */
    public Object getObject(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC SMALLINT parameter as a short
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a short with the parameter value. 0 if the value is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public short getShort(int parameterIndex) throws SQLException;

    /**
     * Returns the indexed parameter's value as a string. The parameter value
     * must be one of the JDBC types <code>CHAR</code>, <code>VARCHAR</code>
     * or <code>LONGVARCHAR</code>.
     * <p>
     * The string corresponding to a <code>CHAR</code> of fixed length will be
     * of identical length to the value in the database inclusive of padding
     * characters.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a String with the parameter value. null if the value is SQL NULL.
     * @throws SQLException
     *             if there is a problem accessing the database
     */
    public String getString(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC TIME parameter as a java.sql.Time.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a java.sql.Time with the parameter value. null if the value is
     *         SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Time getTime(int parameterIndex) throws SQLException;

    /**
     * Gets the value of a specified JDBC TIME parameter as a java.sql.Time,
     * using the supplied Calendar to construct the time. The JDBC driver uses
     * the Calendar to handle specific timezones and locales when creating the
     * Time.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param cal
     *            the Calendar to use in constructing the Time.
     * @return a java.sql.Time with the parameter value. null if the value is
     *         SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException;

    /**
     * Returns the indexed parameter's <code>TIMESTAMP</code> value as a
     * <code>java.sql.Timestamp</code>.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a new <code>java.sql.Timestamp</code> with the parameter value.
     *         A <code>null</code> reference is returned for an SQL value of
     *         <code>NULL</code>
     * @throws SQLException
     *             if a database error happens
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException;

    /**
     * Returns the indexed parameter's <code>TIMESTAMP</code> value as a
     * <code>java.sql.Timestamp</code>. The JDBC driver uses the supplied
     * <code>Calendar</code> to handle specific timezones and locales when
     * creating the result.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param cal
     *            used for creating the returned <code>Timestamp</code>
     * @return a new <code>java.sql.Timestamp</code> with the parameter value.
     *         A <code>null</code> reference is returned for an SQL value of
     *         <code>NULL</code>
     * @throws SQLException
     *             if a database error happens
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal)
            throws SQLException;

    /**
     * Gets the value of a specified JDBC DATALINK parameter as a java.net.URL.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @return a java.sql.Datalink with the parameter value. null if the value
     *         is SQL NULL.
     * @throws SQLException
     *             if a database error happens
     */
    public URL getURL(int parameterIndex) throws SQLException;

    /**
     * Defines the Type of a specified OUT parameter. All OUT parameters must
     * have their Type defined before a stored procedure is executed.
     * <p>
     * The Type defined by this method fixes the Java type that must be
     * retrieved using the getter methods of CallableStatement. If a database
     * specific type is expected for a parameter, the Type java.sql.Types.OTHER
     * should be used. Note that there is another variant of this method for
     * User Defined Types or a REF type.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param sqlType
     *            the JDBC type as defined by java.sql.Types. The JDBC types
     *            NUMERIC and DECIMAL should be defined using the version of
     *            <code>registerOutParameter</code> that takes a
     *            <code>scale</code> parameter.
     * @throws SQLException
     *             if a database error happens
     */
    public void registerOutParameter(int parameterIndex, int sqlType)
            throws SQLException;

    /**
     * Defines the Type of a specified OUT parameter. All OUT parameters must
     * have their Type defined before a stored procedure is executed. This
     * version of the registerOutParameter method, which has a scale parameter,
     * should be used for the JDBC types NUMERIC and DECIMAL, where there is a
     * need to specify the number of digits expected after the decimal point.
     * <p>
     * The Type defined by this method fixes the Java type that must be
     * retrieved using the getter methods of CallableStatement.
     * 
     * @param parameterIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param sqlType
     *            the JDBC type as defined by java.sql.Types.
     * @param scale
     *            the number of digits after the decimal point. Must be greater
     *            than or equal to 0.
     * @throws SQLException
     *             if a database error happens
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
            throws SQLException;

    /**
     * Defines the Type of a specified OUT parameter. This variant of the method
     * is designed for use with parameters that are User Defined Types (UDT) or
     * a REF type, although it can be used for any type.
     * 
     * @param paramIndex
     *            the parameter number index, where the first parameter has
     *            index 1
     * @param sqlType
     *            a JDBC type expressed as a constant from {@link Types}
     * @param typeName
     *            an SQL type name. For a REF type, this name should be the
     *            fully qualified name of the referenced type.
     * @throws SQLException
     *             if a database error happens
     */
    public void registerOutParameter(int paramIndex, int sqlType,
            String typeName) throws SQLException;

    /**
     * Gets whether the value of the last OUT parameter read was SQL NULL.
     * 
     * @return true if the last parameter was SQL NULL, false otherwise.
     * @throws SQLException
     *             if a database error happens
     */
    public boolean wasNull() throws SQLException;
}
