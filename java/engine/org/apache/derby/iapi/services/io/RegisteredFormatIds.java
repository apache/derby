/*

   Derby - Class org.apache.derby.iapi.services.io.RegisteredFormatIds

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

package org.apache.derby.iapi.services.io;

/**
        Registration of TypedFormat classes.

        <P>
        A TypedFormat is registered by placing a class name at the
        correct place in the correct array, driven by the base format number:
        <UL>
        <LI>2 byte - MIN_TWO_BYTE_FORMAT_ID - TwoByte
        </UL>
        The offset from the base format number (0 based) gives the offset in the array.
        <P>
        The class name is either:
        <UL>
        <LI> The actual class name of the TypeFormat.
        <LI> The name of a class that extends org.apache.derby.iapi.services.io.FormatableInstanceGetter.
             In this case the monitor will register an instance of the class after calling its
                 setFormatId() method with format id it is registered as.
        </UL>
*/

public class RegisteredFormatIds {

/* one byte  format identifiers never used
private static final String[] OneByte = {
};
*/

private static final    String[] TwoByte = {
        /* 0 */         null, // null marker
        /* 1 */         null, // String marker
        /* 2 */         null, // Serializable marker
        /* 3 */         null,
        /* 4 */         null,
        /* 5 */         null,
        /* 6 */         null,
        /* 7 */         null,
        /* 8 */         null,
        /* 9 */         null,
        /* 10 */        null,
        /* 11 */        null,
        /* 12 */        null,
        /* 13 */        null,
        /* 14 */        "org.apache.derby.catalog.types.TypeDescriptorImpl",
        /* 15 */        "org.apache.derby.impl.store.access.PC_XenaVersion",
        /* 16 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 17 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 18 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 19 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 20 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 21 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 22 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 23 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 24 */        null,
        /* 25 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 26 */        null,
        /* 27 */        null,
        /* 28 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 29 */        null,
        /* 30 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 31 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 32 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 33 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 34 */        "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 35 */        null,
        /* 36 */        null,
        /* 37 */        "org.apache.derby.impl.sql.execute.DeleteConstantAction",
        /* 38 */        "org.apache.derby.impl.sql.execute.InsertConstantAction",
        /* 39 */        "org.apache.derby.impl.sql.execute.UpdateConstantAction",
        /* 40 */        null,
        /* 41 */        null,
        /* 42 */        null,
        /* 43 */        null,
        /* 44 */        null,
        /* 45 */        null,
        /* 46 */        null,
        /* 47 */        null,
        /* 48 */        null,
        /* 49 */        null,
        /* 50 */        null,
        /* 51 */        null,
        /* 52 */        null,
        /* 53 */        null,
        /* 54 */        null,
        /* 55 */        null,
        /* 56 */        null,
        /* 57 */        null,
        /* 58 */        null,
        /* 59 */        null,
        /* 60 */        null,
        /* 61 */        null,
        /* 62 */        null,
        /* 63 */        null,
        /* 64 */        null,
        /* 65 */        null,
        /* 66 */        null,
        /* 67 */        null,
        /* 68 */        null,
        /* 69 */        null,
        /* 70 */        null,
        /* 71 */        null,
        /* 72 */        null,
        /* 73 */        null, 
        /* 74 */        null,
        /* 75 */        null,
        /* 76 */        null,
        /* 77 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 78 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 79 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 80 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 81 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 82 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 83 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 84 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 85 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 86 */        null,
        /* 87 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 88 */        "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 89 */        "org.apache.derby.iapi.types.SQLLongint",
        /* 90 */        "org.apache.derby.impl.store.access.heap.HeapClassInfo",
        /* 91 */        "org.apache.derby.impl.store.access.heap.Heap_v10_2",
        /* 92 */        null,
        /* 93 */        "org.apache.derby.impl.store.access.StorableFormatId",
        /* 94 */        null,
        /* 95 */        "org.apache.derby.impl.store.access.btree.index.B2IUndo",
        /* 96 */        null,
        /* 97 */        "org.apache.derby.impl.store.raw.data.ChainAllocPageOperation",
        /* 98 */        null,
        /* 99 */        null,
        /* 100 */       null,
        /* 101 */       "org.apache.derby.impl.store.raw.data.DeleteOperation",
        /* 102 */       "org.apache.derby.impl.store.raw.xact.EndXact",
        /* 103 */       "org.apache.derby.impl.store.raw.data.InsertOperation",
        /* 104 */       "org.apache.derby.impl.store.raw.data.LogicalUndoOperation",
        /* 105 */       "org.apache.derby.impl.store.raw.data.PhysicalUndoOperation",
        /* 106 */       "org.apache.derby.impl.store.raw.data.PurgeOperation",
        /* 107 */       "org.apache.derby.impl.store.raw.data.ContainerUndoOperation",
        /* 108 */       "org.apache.derby.impl.store.raw.data.UpdateOperation",
        /* 109 */       "org.apache.derby.impl.store.raw.data.UpdateFieldOperation",
        /* 110 */       null,
        /* 111 */       "org.apache.derby.impl.store.raw.data.AllocPageOperation",
        /* 112 */       null,
        /* 113 */       "org.apache.derby.impl.store.raw.data.InvalidatePageOperation",
        /* 114 */       null,
        /* 115 */       null, 
        /* 116 */       null,
        /* 117 */       "org.apache.derby.impl.store.raw.data.StoredPage",
        /* 118 */       "org.apache.derby.impl.store.raw.data.AllocPage",
        /* 119 */       null,
        /* 120 */       null,
        /* 121 */       null,
        /* 122 */       null,
        /* 123 */       null,
        /* 124 */       null,
        /* 125 */       null,
        /* 126 */       null,
        /* 127 */       null,
        /* 128 */       null,
        /* 129 */       "org.apache.derby.impl.store.raw.log.LogRecord",
        /* 130 */       "org.apache.derby.impl.store.raw.log.LogCounter",
        /* 131 */       "org.apache.derby.impl.services.uuid.BasicUUIDGetter",           // InstanceGetter
        /* 132 */       null,
        /* 133 */       "org.apache.derby.impl.store.access.btree.LeafControlRow",
        /* 134 */       "org.apache.derby.impl.store.access.btree.BranchControlRow",
        /* 135 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 136 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 137 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 138 */       null,
        /* 139 */       null,
        /* 140 */       null,
        /* 141 */       null,
        /* 142 */       null,
        /* 143 */       null,
        /* 144 */       null,
        /* 145 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 146 */       null,
        /* 147 */       "org.apache.derby.impl.store.raw.xact.XactId",
        /* 148 */       null,
        /* 149 */       "org.apache.derby.impl.sql.execute.AvgAggregator",
        /* 150 */       null,
        /* 151 */       "org.apache.derby.impl.sql.execute.CountAggregator",
        /* 152 */       "org.apache.derby.impl.sql.execute.MaxMinAggregator",
        /* 153 */       null,
        /* 154 */       "org.apache.derby.impl.sql.execute.SumAggregator",
        /* 155 */       null,
        /* 156 */       null, 
        /* 157 */       null, 
        /* 158 */       null, 
        /* 159 */       null, 
        /* 160 */       null, 
        /* 161 */       null, 
        /* 162 */       null, 
        /* 163 */       null, 
        /* 164 */       null, 
        /* 165 */       null, 
        /* 166 */       null, 
        /* 167 */       null, 
        /* 168 */       null, 
        /* 169 */       "org.apache.derby.impl.store.raw.xact.BeginXact",
        /* 170 */       null, 
        /* 171 */       null, 
        /* 172 */       null,
        /* 173 */       null,
        /* 174 */       null,
        /* 175 */       null,
        /* 176 */       null,
        /* 177 */       null,
        /* 178 */       null,
        /* 179 */       null,
        /* 180 */       null,
        /* 181 */       null,
        /* 182 */       null,
        /* 183 */       null,
        /* 184 */       null,
        /* 185 */       null,
        /* 186 */       null,
        /* 187 */       null,
        /* 188 */       null,
        /* 189 */       null,
        /* 190 */       null,
        /* 191 */       null,
        /* 192 */       null,
        /* 193 */       null,
        /* 194 */       null,
        /* 195 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 196 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 197 */       null,
        /* 198 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 199 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 200 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 201 */       null,
        /* 202 */       "org.apache.derby.iapi.types.UserType",
        /* 203 */       null,
        /* 204 */       null,
        /* 205 */       "org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl",
        /* 206 */       null,
        /* 207 */       null,
        /* 208 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 209 */       null,
        /* 210 */       "org.apache.derby.impl.store.raw.data.CopyRowsOperation",
        /* 211 */       null,
        /* 212 */       null,
        /* 213 */       null,
        /* 214 */       null,
        /* 215 */       null,
        /* 216 */       null,
        /* 217 */       null,
        /* 218 */       "org.apache.derby.impl.sql.execute.IndexColumnOrder",
        /* 219 */       null,
        /* 220 */       null,
        /* 221 */       null,
        /* 222 */       null,
        /* 223 */       "org.apache.derby.impl.sql.execute.AggregatorInfo",
        /* 224 */       "org.apache.derby.impl.sql.execute.AggregatorInfoList",
        /* 225 */       "org.apache.derby.impl.sql.GenericStorablePreparedStatement",
        /* 226 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 227 */       null,
        /* 228 */       "org.apache.derby.impl.sql.GenericResultDescription",
        /* 229 */       null,
        /* 230 */       null,
        /* 231 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 232 */       null,
        /* 233 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 234 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 235 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
        /* 236 */       null,
        /* 237 */       null,
        /* 238 */       null,
        /* 239 */       null,
        /* 240 */       "org.apache.derby.iapi.types.DataTypeDescriptor",
        /* 241 */       "org.apache.derby.impl.store.raw.data.InitPageOperation",
        /* 242 */       "org.apache.derby.impl.store.raw.data.ContainerOperation",
        /* 243 */       null,
        /* 244 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 245 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 246 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 247 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 248 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 249 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 250 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 251 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 252 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 253 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 254 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 255 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 256 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 257 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 258 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 259 */       "org.apache.derby.catalog.types.TypesImplInstanceGetter", // old catalog type format
        /* 260 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 261 */       "org.apache.derby.impl.store.raw.xact.TransactionTableEntry",
        /* 262 */       "org.apache.derby.impl.store.raw.xact.TransactionTable",
        /* 263 */       "org.apache.derby.impl.store.raw.log.CheckpointOperation",
        /* 264 */       "org.apache.derby.catalog.types.UserDefinedTypeIdImpl",
        /* 265 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 266 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 267 */       null,
        /* 268 */       "org.apache.derby.iapi.sql.dictionary.IndexRowGenerator",
        /* 269 */       "org.apache.derby.iapi.services.io.FormatableBitSet",
        /* 270 */       "org.apache.derby.iapi.services.io.FormatableArrayHolder",
        /* 271 */       "org.apache.derby.iapi.services.io.FormatableProperties",
        /* 272 */       null,
        /* 273 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 274 */       null,
        /* 275 */       null,
        /* 276 */       null,
        /* 277 */       null,
        /* 278 */       "org.apache.derby.impl.sql.execute.ConstraintInfo",
        /* 279 */       null,
        /* 280 */       null,
        /* 281 */       null,
        /* 282 */       "org.apache.derby.impl.sql.execute.FKInfo",
        /* 283 */       null,
        /* 284 */       null,
        /* 285 */       null,
        /* 286 */       null,
        /* 287 */       "org.apache.derby.impl.store.raw.data.SetReservedSpaceOperation",
        /* 288 */    null,
        /* 289 */       null,
        /* 290 */       null,
        /* 291 */       "org.apache.derby.impl.store.raw.data.RemoveFileOperation",
        /* 292 */       null,
        /* 293 */       null,
        /* 294 */       null,
        /* 295 */       null,
        /* 296 */       "org.apache.derby.impl.sql.CursorTableReference",
        /* 297 */       "org.apache.derby.impl.sql.CursorInfo",
        /* 298 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 299 */       "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 300 */       null,
        /* 301 */       null,
        /* 302 */       null,
        /* 303 */       "org.apache.derby.iapi.services.io.FormatableIntHolder",
        /* 304 */       null,
        /* 305 */       null,
        /* 306 */       null,
        /* 307 */       "org.apache.derby.iapi.types.JSQLType",
        /* 308 */       null,
        /* 309 */       null,
        /* 310 */       null,
        /* 311 */       null,
        /* 312 */       "org.apache.derby.catalog.types.MethodAliasInfo",
        /* 313 */       "org.apache.derby.iapi.services.io.FormatableHashtable",
        /* 314 */       null,
        /* 315 */       null,
        /* 316 */       "org.apache.derby.iapi.sql.dictionary.TriggerDescriptor",
        /* 317 */       "org.apache.derby.impl.sql.execute.TriggerInfo",
        /* 318 */       null,
        /* 319 */       null,
        /* 320 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 321 */       null,
        /* 322 */       null,
        /* 323 */       "org.apache.derby.impl.sql.execute.UserDefinedAggregator",
        /* 324 */       null,
        /* 325 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 326 */       "org.apache.derby.catalog.types.DefaultInfoImpl",
        /* 327 */       null,
        /* 328 */       "org.apache.derby.impl.store.raw.xact.GlobalXactId",
        /* 329 */       "org.apache.derby.iapi.services.io.FormatableLongHolder",
        /* 330 */       null,
        /* 331 */       null,
        /* 332 */       null,
        /* 333 */       null,
        /* 334 */       null,
        /* 335 */       null,
        /* 336 */       null,
        /* 337 */       null,
        /* 338 */       null,
        /* 339 */       null,
        /* 340 */       null,
        /* 341 */       null,
        /* 342 */       null,
        /* 343 */       null,
        /* 344 */       null,
        /* 345 */       null,
        /* 346 */       null,
        /* 347 */       null,
        /* 348 */       null,
        /* 349 */       null,
        /* 350 */       null,
        /* 351 */       null,
        /* 352 */       null,
        /* 353 */       null,
        /* 354 */       null,
        /* 355 */       null,
        /* 356 */       null,
        /* 357 */       null,
        /* 358 */       "org.apache.derby.impl.sql.execute.ColumnInfo",
        /* 359 */       "org.apache.derby.impl.sql.depend.DepClassInfo",
        /* 360 */       "org.apache.derby.impl.store.access.btree.index.B2IStaticCompiledInfo",
        /* 361 */       null, // SQLData marker
        /* 362 */       null,
        /* 363 */       null,
        /* 364 */       null,
        /* 365 */       null,
        /* 366 */       null,
        /* 367 */       null,
        /* 368 */       null,
        /* 369 */       null,
        /* 370 */       null,
        /* 371 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 372 */       null,
        /* 373 */       null,
        /* 374 */       null,
        /* 375 */       "org.apache.derby.impl.sql.execute.UpdatableVTIConstantAction",
        /* 376 */       null,
        /* 377 */       null,
        /* 378 */       null,
        /* 379 */       null,
        /* 380 */       null,
        /* 381 */       null, // Unused,
        /* 382 */       null, // Unused
        /* 383 */   "org.apache.derby.impl.sql.GenericColumnDescriptor",
        /* 384 */   null, // Unused,
        /* 385 */   null,
        /* 386 */   null,
        /* 387 */       "org.apache.derby.catalog.types.IndexDescriptorImpl",
        /* 388 */       "org.apache.derby.impl.store.access.btree.index.B2I_v10_2",
        /* 389 */   null,
        /* 390 */   null,
        /* 391 */   null,
        /* 392 */   null,
        /* 393 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",             // InstanceGetter
        /* 394 */   null,
        /* 395 */   null,
        /* 396 */       null, // Unused
        /* 397 */   "org.apache.derby.catalog.types.StatisticsImpl",
        /* 398 */       null,
        /* 399 */       null,
        /* 400 */   null,
        /* 401 */       "org.apache.derby.impl.sql.catalog.DD_Version",
        /* 402 */       "org.apache.derby.impl.sql.catalog.DD_Version",
        /* 403 + 0 */           null,
        /* 1 */         null,
        /* 2 */         null,
        /* 3 */         null,
        /* 4 */         null,
        /* 5 */         null,
        /* 6 */         null,
        /* 7 */         null,
        /* 8 */         null,
        /* 9 */         null,
        /* 10 */        null,
        /* 11 */        null,
        /* 12 */        null,
        /* 13 */        null,
        /* 14 */        null,
        /* 15 */        null,
        /* 16 */        null,
        /* 17 */        null,
        /* 18 */        null,
        /* 19 */        null,
        /* 20 */        null,
        /* 21 */        null,
        /* 22 */        null,
        /* 23 */        null,
        /* 24 */        null,
        /* 25 */        null,
        /* 26 */        null,
        /* 27 */        null,
        /* 28 */        null,
        /* 29 */        null,
        /* 30 */        null,
        /* 31 */        null,
        /* 32 */        null,
        /* 33 */        null,
        /* 403 + 34 */  null,
        /* 438 */       null,
        /* 439 */       null,

    /// --- BLOB is copying LONGVARBIT in implementation
        /* 440 */   null,
        /* 441 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, BLOB_COMPILATION_TYPE_ID
        /* 442 */   "org.apache.derby.catalog.types.TypesImplInstanceGetter", // BLOB_TYPE_ID_IMPL
        /* 443 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, SQL_BLOB_ID

    /// --- CLOB is copying LONGVARCHAR in implementation
        /* 444 */   null,
        /* 445 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter
        /* 446 */   "org.apache.derby.catalog.types.TypesImplInstanceGetter",
        /* 447 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter,
           

    /// --- NLOB is copying NATIONAL LONGVARCHAR in implementation
        
        /* 448 */   null,
        /* 449 */   null,
        /* 450 */   null,

        /* 451 */   "org.apache.derby.catalog.types.RoutineAliasInfo",
		/* 452 */   null,
		/* 453 */   "org.apache.derby.impl.store.raw.log.ChecksumOperation",
		/* 454 */   "org.apache.derby.impl.store.raw.data.CompressSpacePageOperation10_2",
		/* 455 */   "org.apache.derby.catalog.types.SynonymAliasInfo",
        /* 456 */   null,
        /* 457 */   "org.apache.derby.catalog.types.TypesImplInstanceGetter", // XML_TYPE_ID_IMPL
        /* 458 */   "org.apache.derby.iapi.types.DTSClassInfo", //InstanceGetter, XML_ID
        /* 459 */   "org.apache.derby.impl.store.raw.data.EncryptContainerOperation",
        /* 460 */   "org.apache.derby.impl.store.raw.data.EncryptContainerUndoOperation",
        /* 461 */   "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 462 */   "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 463 */   "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 464 */   null,
		/* 465 */   "org.apache.derby.impl.store.raw.data.CompressSpacePageOperation",
        /* 466 */   "org.apache.derby.impl.store.access.btree.index.B2I_10_3",
        /* 467 */   "org.apache.derby.impl.store.access.heap.Heap",
        /* 468 */   "org.apache.derby.iapi.types.DTSClassInfo",
        /* 469 */   "org.apache.derby.catalog.types.RowMultiSetImpl",
        /* 470 */   "org.apache.derby.impl.store.access.btree.index.B2I",
        /* 471 */   "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 472 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 473 */       "org.apache.derby.impl.sql.catalog.CoreDDFinderClassInfo",
        /* 474 */       "org.apache.derby.catalog.types.UDTAliasInfo",
        /* 475 */       "org.apache.derby.catalog.types.AggregateAliasInfo",
};

    /** Return the number of two-byte format ids */
    public  static  int countTwoByteIDs() { return TwoByte.length; }

    /** Return the class name bound to an index into TwoByte */
    public  static  String  classNameForTwoByteID( int idx ) { return TwoByte[ idx ]; }
}
