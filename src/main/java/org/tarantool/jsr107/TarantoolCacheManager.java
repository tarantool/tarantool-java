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

package org.tarantool.jsr107;

import org.tarantool.SocketChannelProvider;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;
import org.tarantool.jsr107.TarantoolCachingProvider;
import org.tarantool.jsr107.TarantoolCache.TarantoolIterator;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of the {@link CacheManager}.
 * TarantoolCacheManager associated with one Tarantool instance.
 * URI for each TarantoolCacheManager should be unique
 * and should consist of URI connection path to Tarantool instance:
 * tarantool://user:password@host:port
 * 
 * If password and(or) user is empty, defaults would be taken.
 * If host is empty, 'localhost' would be taken.
 * If port is empty, default Tarantool port 3301 would be taken.
 * 
 * @author Yannis Cosmadopoulos
 * @author Brian Oliver
 * @author Evgeniy Zaikin
 * @since 1.0
 */
public class TarantoolCacheManager implements CacheManager {

  private static final Logger LOGGER = Logger.getLogger("javax.cache");
  private final HashMap<String, TarantoolCache<?, ?>> caches = new HashMap<String, TarantoolCache<?, ?>>();

  private final TarantoolCachingProvider cachingProvider;
  private final TarantoolClientImpl tarantoolClient;

  private final URI uri;
  private final WeakReference<ClassLoader> classLoaderReference;
  private final Properties properties;

  private volatile boolean isClosed;

  private void ensureOpen() {
      if (isClosed()) {
          throw new IllegalStateException("The cache is closed");
      }
  }

  /**
   * Constructs a new TarantoolCacheManager with the specified name.
   *
   * @param cachingProvider the CachingProvider that created the CacheManager
   * @param uri             the name of this cache manager
   * @param classLoader     the ClassLoader that should be used in converting values into Java Objects.
   * @param properties      the vendor specific Properties for the CacheManager
   * @throws NullPointerException if the URI and/or classLoader is null.
   */
  public TarantoolCacheManager(TarantoolCachingProvider cachingProvider, URI uri, ClassLoader classLoader, Properties properties) {
    this.cachingProvider = cachingProvider;

    if (uri == null) {
      throw new NullPointerException("No CacheManager URI specified");
    }
    this.uri = uri;

    if (classLoader == null) {
      throw new NullPointerException("No ClassLoader specified");
    }
    this.classLoaderReference = new WeakReference<ClassLoader>(classLoader);

    this.properties = new Properties();
    //todo: decide, shall we use given 'properties' as default properties?
    //this.properties = properties == null ? new Properties() : new Properties(properties);
    if (properties != null) {
      this.properties.putAll(properties);
    }

    //todo: take TarantoolClientConfig parameters from project's .xml or .properties file
    TarantoolClientConfig config = new TarantoolClientConfig();
    String userinfo = uri.getUserInfo();
    if (userinfo != null) {
        int pos = userinfo.indexOf(':');
        if (pos < 0) {
            config.username = userinfo;
        } else {
            config.username = userinfo.substring(0, pos);
            config.password = userinfo.substring(pos + 1);
        }
    }

    config.initTimeoutMillis = 1000;
    config.writeTimeoutMillis = 2;
    config.sharedBufferSize = 128;
    config.useNewCall = true;

    SocketChannelProvider socketChannelProvider = new SocketChannelProvider() {
        @Override
        public SocketChannel get(int retryNumber, Throwable lastError) {
            if (lastError != null) {
                lastError.printStackTrace(System.out);
            }
            try {
                String host = uri.getHost() != null ? uri.getHost() : "localhost";
                int port = uri.getPort() != -1 ? uri.getPort() : 3301;
                return SocketChannel.open(new InetSocketAddress(host, port));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    tarantoolClient = new TarantoolClientImpl(socketChannelProvider, config) {{
        msgPackLite = JSRMsgPackLite.INSTANCE;
    }};
    tarantoolClient.syncOps().ping();

    isClosed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CachingProvider getCachingProvider() {
    return cachingProvider;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close() {
    if (!isClosed()) {
      //first releaseCacheManager the CacheManager from the CacheProvider so that
      //future requests for this CacheManager won't return this one
      cachingProvider.releaseCacheManager(getURI(), getClassLoader());
      ArrayList<Cache<?, ?>> cacheList;
      synchronized (caches) {
        cacheList = new ArrayList<Cache<?, ?>>(caches.values());
        caches.clear();
      }
      for (Cache<?, ?> cache : cacheList) {
        try {
          cache.close();
        } catch (Exception e) {
          getLogger().log(Level.WARNING, "Error stopping cache: " + cache, e);
        }
      }
      tarantoolClient.close();
      isClosed = true;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public URI getURI() {
    return uri;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getProperties() {
    return properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return classLoaderReference.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) {
    ensureOpen();

    if (cacheName == null) {
      throw new NullPointerException("cacheName must not be null");
    }

    if (configuration == null) {
      throw new NullPointerException("configuration must not be null");
    }

    synchronized (caches) {
      if (caches.get(cacheName) == null) {
        TarantoolCache<K, V> cache = new TarantoolCache<K,V>(this, cacheName, getClassLoader(), configuration);
        caches.put(cache.getName(), cache);
        return cache;
      } else {
        throw new CacheException("A cache named " + cacheName + " already exists.");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
    ensureOpen();

    if (cacheName == null) {
        throw new NullPointerException("cacheName can not be null");
    }

    if (keyType == null) {
      throw new NullPointerException("keyType can not be null");
    }

    if (valueType == null) {
      throw new NullPointerException("valueType can not be null");
    }

    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      return null;
    } else {
      Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);

      if (configuration.getKeyType() != null &&
          configuration.getKeyType().equals(keyType)) {

        if (configuration.getValueType() != null &&
            configuration.getValueType().equals(valueType)) {

          return (Cache<K, V>) cache;
        } else {
          throw new ClassCastException("Incompatible cache value types specified, expected " +
              configuration.getValueType() + " but " + valueType + " was specified");
        }
      } else {
        throw new ClassCastException("Incompatible cache key types specified, expected " +
            configuration.getKeyType() + " but " + keyType + " was specified");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName) {
    ensureOpen();
    if (cacheName == null) {
        throw new NullPointerException();
    }
    return (Cache<K, V>) caches.get(cacheName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getCacheNames() {
    ensureOpen();
    synchronized (caches) {
      HashSet<String> set = new HashSet<String>(caches.keySet());
      return Collections.unmodifiableSet(set);
    }
  }

  /**
   * {@inheritDoc}
   */
  public void destroyCache(String cacheName) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }

    Cache<?, ?> cache = caches.get(cacheName);

    if (cache != null) {
      cache.close();
    }
  }

  /**
   * Releases the Cache with the specified name from being managed by
   * this CacheManager.
   *
   * @param cacheName the name of the Cache to releaseCacheManager
   */
  void releaseCache(String cacheName) {
    if (cacheName == null) {
      throw new NullPointerException();
    }
    synchronized (caches) {
      caches.remove(cacheName);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableStatistics(String cacheName, boolean enabled) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }
    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }
    cache.setStatisticsEnabled(enabled);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }
    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }
    cache.setManagementEnabled(enabled);
  }

  @Override
  public <T> T unwrap(java.lang.Class<T> cls) {
    if (cls.isAssignableFrom(getClass())) {
      return cls.cast(this);
    }

    throw new IllegalArgumentException("Unwapping to " + cls + " is not a supported by this implementation");
  }

  /**
   * Obtain the logger.
   *
   * @return the logger.
   */
  Logger getLogger() {
    return LOGGER;
  }

  /**
   * Execute "select" request.
   * @param spaceId int Tarantool space.id
   * @param keys List<?> keys
   * @param iterator TarantoolIterator
   * @param int limit max size of batch per select
   * @return List<?> as response.
   */
  List<?> select(int spaceId, List<?> keys, TarantoolIterator iterator, int limit) {
    try {
        return tarantoolClient.syncOps().select(spaceId, 0, keys, 0, limit, iterator.ordinal());
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }

  /**
   * Execute update request.
   * @param spaceId int Tarantool space.id
   * @param keys List<?> keys
   * @param Object... ops operations for update
   * @return List<?> list of updated tuples.
   */
  List<?> update(int spaceId, List<?> keys, Object... ops) {
    try {
        return tarantoolClient.syncOps().update(spaceId, keys, ops);
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }

  /**
   * Execute "update or insert" request.
   * @param spaceId int Tarantool space.id
   * @param keys List<?> keys
   * @param defTuple List<?> tuple to insert (if not exists yet)
   * @param Object... ops operations for update (if tuple exists)
   */
  void upsert(int spaceId, List<?> keys, List<?> defTuple, Object... ops) {
    try {
        tarantoolClient.syncOps().upsert(spaceId, keys, defTuple, ops);
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }

  /**
   * Execute "delete" request.
   * @param spaceId int Tarantool space.id
   * @param keys List<?> keys
   * @return List<?> as list of actually deleted tuples.
   */
  List<?> delete(int spaceId, List<?> keys) {
    try {
        return tarantoolClient.syncOps().delete(spaceId, keys);
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }

  /**
   * Execute and evaluate.
   * @param expression string
   * @return List<?> as response.
   */
  List<?> execute(String expression) {
    try {
        return tarantoolClient.syncOps().eval("return " + expression);
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }

  /**
   * Call Tarantool function.
   * @param function string
   * @return List<?> as response.
   */
  List<?> call(String function, Object... args) {
    try {
        return tarantoolClient.syncOps().call(function, args);
    } catch (Exception e) {
        throw new CacheException(e);
    }
  }
}
