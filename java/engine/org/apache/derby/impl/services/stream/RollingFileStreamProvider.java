/*

 Derby - Class org.apache.derby.iapi.services.stream.RollingFileStreamProvider

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */
package org.apache.derby.impl.services.stream;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;

/**
 * Creates and configures a RollingFileStream
 *
 * @author brett
 */
public class RollingFileStreamProvider {

    /**
     * Creates and returns the OutputStream for a RollingFileStream.
     * The <tt>derbylog.properties</tt> file contains the configuration. If the
     * file is not found, then hard coded default values are used to configure
     * the RollingFileStream. <p>The following properties can be specified <dl>
     * <dt>pattern</dt> <dd>The pattern to use, the default is
     * <tt>%d/derby-%g.log</tt></dd> <dt>limit</dt> <dd>The file size limit, the
     * default is <tt>1024000</tt></dd> <dt>count</dt> <dd>The file count, the
     * default is <tt>10</tt></dd> <dt>append</dt> <dd>If true the last logfile
     * is appended to, the default is <tt>true</tt></dd>
     *
     * @return The configured OutputStream
     * @throws IOException
     * @throws SecurityException  
     */
    public static OutputStream getOutputStream() throws IOException, SecurityException {
        OutputStream res = null;

        String pattern = PropertyUtil.getSystemProperty(Property.ERRORLOG_ROLLINGFILE_PATTERN_PROPERTY, "%d/derby-%g.log");
        int limit = Integer.parseInt(PropertyUtil.getSystemProperty(Property.ERRORLOG_ROLLINGFILE_LIMIT_PROPERTY, "1024000"));
        int count = Integer.parseInt(PropertyUtil.getSystemProperty(Property.ERRORLOG_ROLLINGFILE_COUNT_PROPERTY, "10"));
        boolean append = Boolean.parseBoolean(PropertyUtil.getSystemProperty(Property.LOG_FILE_APPEND, "true"));

        RollingFileStream rfh = new RollingFileStream(pattern, limit, count, append);
        res = rfh;

        return res;
    }
}
