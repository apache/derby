/*
 *
 * Derby - Class org.apache.derbyTesting.junit.OsName
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.junit;

/**
 * OsName is used to store constants for the System Property os.name 
 * that can be passed to the BaseTestCase.isPlatform(String) method.
 * Started this class with a few known values.
 * TODO: Expand for all known os.names for platforms running Derby tests
 *
 */
public class OsName {

    
    public static final String LINUX = "Linux";
    public static final String MACOS = "Mac OS";
    public static final String MACOSX = "Mac OS X";
    public static final String AIX = "AIX";
    public static final String OS400 = "OS/400";
    public static final String ZOS = "z/OS";
    public static final String WINDOWSXP = "Windows XP";
    
    
}
