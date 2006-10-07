/*
 
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

/**
 * Encapsulates a request that we're posting to Google Calendar.
 * Subtypes provide type-safe mechanisms for providing parameters
 * to the request and for the result data, if any.
 */
public abstract class GCalendarRequest implements java.io.Serializable {  
    private int requestId;
    
    public GCalendarRequest(int id) {
        setId(id);
    }
    
    public void setId(int id) {
        this.requestId = id;
    }
    
    public int getId() {
        return requestId;
    }
    
    /**
     * Submit the request to the Google Calendar service.  What this
     * means depends on the type of request.
     */
    public abstract void submit(GCalendar calendar) throws Exception ;
    
    public abstract String toString();
}
