/**
 *  Copyright 2011-2013 Terracotta, Inc.
 *  Copyright 2011-2013 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.tarantool.jsr107.processor;

import javax.cache.processor.MutableEntry;

/**
 * The operation to perform on a {@link org.tarantool.jsr107.CachedValue} as a result of
 * actions performed on a {@link MutableEntry}.
 */
public enum MutableEntryOperation {
  /**
   * Don't perform any operations on the {@link org.tarantool.jsr107.CachedValue}.
   */
  NONE,

  /**
   * Access an existing {@link  org.tarantool.jsr107.CachedValue}.
   */
  ACCESS,

  /**
   * Create a new {@link org.tarantool.jsr107.CachedValue}.
   */
  CREATE,

  /**
   * Loaded a new {@link org.tarantool.jsr107.CachedValue}.
   */
  LOAD,

  /**
   * Remove the {@link org.tarantool.jsr107.CachedValue} (and thus the Cache Entry).
   */
  REMOVE,

  /**
   * Update the {@link org.tarantool.jsr107.CachedValue}.
   */
  UPDATE
}
