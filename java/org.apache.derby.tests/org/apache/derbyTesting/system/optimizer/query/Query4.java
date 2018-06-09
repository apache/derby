/*
 
 Derby - Class Class org.apache.derbyTesting.system.langtest.query.Query4
 
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
package org.apache.derbyTesting.system.optimizer.query;

/**
 * 
 * Class Query4: Returns a list of queries that Selects from multiple views with
 * joins on columns having no indexes
 * 
 */

public class Query4 extends GenericQuery {

	public Query4() {
		description = "Select from multiple views with joins on columns having no indexes";
		generateQueries();
	}

	/**
	 */
	public void generateQueries() {
		queries
				.add("select v8.col5, v8.col2 , v8_2.col3  from v8 inner join v8_2 on v8.col5=v8_2.col5 where (v8.col1>100 and v8.col1<110) union all select v8.col4, v8.col7 , v8_2.col7  from v8 inner join v8_2 on v8.col6=v8_2.col6 where (v8.col1>100 and v8.col1<110)");
		queries
				.add("select v16.col5, v16.col2 , v16_2.col3  from v16 inner join v16_2 on v16.col5=v16_2.col5 where (v16.col1>100 and v16.col1<110) union all select v16.col4, v16.col7 , v16_2.col7  from v16 inner join v16_2 on v16.col6=v16_2.col6 where (v16.col1>100 and v16.col1<110)");
		queries
				.add("select v32.col5, v32.col2 , v32_2.col3  from v32 inner join v32_2 on v32.col5=v32_2.col5 where (v32.col1>100 and v32.col1<110) union all select v32.col4, v32.col7 , v32_2.col7  from v32 inner join v32_2 on v32.col6=v32_2.col6 where (v32.col1>100 and v32.col1<110)");
		queries
				.add("select v42.col5, v42.col2 , v42_2.col3  from v42 inner join v42_2 on v42.col5=v42_2.col5 where (v42.col1>100 and v42.col1<110) union all select v42.col4, v42.col7 , v42_2.col7  from v42 inner join v42_2 on v42.col6=v42_2.col6 where (v42.col1>100 and v42.col1<110)");

	}

}
