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

This directory contains classes for testing SQL statements invoked
from static initializers.  When these classes are referred to during
binding, they are loaded, causing their static initializers to run.
This can cause nested binding.  Also, DDL is not allowed while binding
is going on.

InsertInStaticInitializer inserts a row into a table from a static Initializer;
i.e
	select staticInitializer.f() from ....;  [q1]
	staticInitialzer.f() executes SQL [q2] which does an insert.
We test that 

1. Locks are held on the table on which the insert is done.
2. Locks are *not* held in the system catalogs which are read for compiling
Q1 and Q2.

