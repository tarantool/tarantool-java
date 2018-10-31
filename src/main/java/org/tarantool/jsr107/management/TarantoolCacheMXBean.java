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
package org.tarantool.jsr107.management;

import javax.cache.Cache;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.management.CacheMXBean;

/**
 * Class to help implementers
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Yannis Cosmadopoulos
 * @author Evgeniy Zaikin
 * @since 1.0
 */
public class TarantoolCacheMXBean<K, V> implements CacheMXBean {

  private final Cache<K, V> cache;

  @SuppressWarnings("unchecked")
  private CompleteConfiguration<?, ?> getCompleteConfiguration() {
    return cache.getConfiguration(CompleteConfiguration.class);
  }

  /**
   * Constructor
   *
   * @param cache the cache
   */
  public TarantoolCacheMXBean(Cache<K, V> cache) {
    this.cache = cache;
  }

  @Override
  public String getKeyType() {
    return getCompleteConfiguration().getKeyType().getName();
  }

  @Override
  public String getValueType() {
    return getCompleteConfiguration().getValueType().getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadThrough() {
    return getCompleteConfiguration().isReadThrough();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWriteThrough() {
    return getCompleteConfiguration().isWriteThrough();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStoreByValue() {
    return getCompleteConfiguration().isStoreByValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStatisticsEnabled() {
    return getCompleteConfiguration().isStatisticsEnabled();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isManagementEnabled() {
    return getCompleteConfiguration().isManagementEnabled();
  }
}
