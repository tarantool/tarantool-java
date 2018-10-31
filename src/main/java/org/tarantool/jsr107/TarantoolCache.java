/**
 * Copyright 2011-2013 Terracotta, Inc.
 * Copyright 2011-2013 Oracle America Incorporated
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tarantool.jsr107;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.jsr107.management.MBeanServerRegistrationUtility;
import org.tarantool.jsr107.management.TarantoolCacheMXBean;
import org.tarantool.jsr107.management.TarantoolCacheStatisticsMXBean;
import org.tarantool.jsr107.processor.EntryProcessorEntry;
import org.tarantool.jsr107.processor.MutableEntryOperation;
import org.tarantool.jsr107.processor.TarantoolEntryProcessorResult;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.*;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.tarantool.jsr107.management.MBeanServerRegistrationUtility.ObjectNameType.Configuration;
import static org.tarantool.jsr107.management.MBeanServerRegistrationUtility.ObjectNameType.Statistics;

/**
 * The implementation of the {@link Cache}.
 * Each TarantoolCache associated with Tarantool's {space_object},
 * (See 'box.space.{space_object_name}' in Tarantool's help)
 * So every TarantoolCache, successfully created from within {@link TarantoolCacheManager},
 * means {space_object} with given name exists within corresponding Tarantool instance.
 * 
 * For every newly created {space_object} key field and hash index for this key field are built.
 * 
 * But when {@link TarantoolCache} or {@link TarantoolCacheManager} are about to be closed,
 * we shouldn't drop {space_object} in order to prevent performance degradation.
 * We should provide cache element eviction/expiration policy instead.
 * 
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Brian Oliver
 * @author Greg Luck
 * @author Yannis Cosmadopoulos
 * @author Evgeniy Zaikin
 */
public final class TarantoolCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(TarantoolCache.class);

    /**
     * The name of the {@link Cache} as used with in the scope of the
     * Cache Manager.
     */
    private final String cacheName;

    /**
     * The {@link CacheManager} that created this implementation
     */
    private final TarantoolCacheManager cacheManager;

    /**
     * The {@link Configuration} for the {@link Cache}.
     */
    private final MutableConfiguration<K, V> configuration;

    /**
     * The {@link CacheLoader} for the {@link Cache}.
     */
    private CacheLoader<K, V> cacheLoader;

    /**
     * The {@link CacheWriter} for the {@link Cache}.
     */
    private CacheWriter<K, V> cacheWriter;

    /**
     * The name of the current space in Tarantool
     */
    private final String spaceName;

    /**
     * The ID of the current space in Tarantool
     */
    private final Integer spaceId;

    /**
     * The {@link ExpiryPolicy} for the {@link Cache}.
     */
    private final ExpiryPolicy expiryPolicy;

    /**
     * The open/closed state of the Cache.
     */
    private volatile boolean isClosed;

    private final TarantoolCacheMXBean<K, V> cacheMXBean;
    private final TarantoolCacheStatisticsMXBean statistics;

    /**
     * An {@link ExecutorService} for the purposes of performing asynchronous
     * background work.
     */
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    private static final long EXECUTOR_WAIT_TIMEOUT = 10;

    private static final int MAX_ROWS_PER_ITER_ALL = 65535;
    //TO-DO: Add "limit" parameter to JCache adjustable properties
    private static final int MAX_ROWS_PER_ITER_ONE = 1;

    public enum TarantoolIterator {
        /* ITER_EQ must be the first member for request_create  */
        ITER_EQ, /* key == x ASC order                  */
        ITER_REQ, /* key == x DESC order                 */
        ITER_ALL, /* all tuples                          */
        ITER_LT, /* key <  x                            */
        ITER_LE, /* key <= x                            */
        ITER_GE, /* key >= x                            */
        ITER_GT, /* key >  x                            */
        ITER_BITS_ALL_SET, /* all bits from x are set in key      */
        ITER_BITS_ANY_SET, /* at least one x's bit is set         */
        ITER_BITS_ALL_NOT_SET, /* all bits are not set                */
        ITER_OVERLAPS, /* key overlaps x                      */
        ITER_NEIGHBOR, /* tuples in distance ascending order from specified point */
    }

    /**
     * Constructs a cache.
     *
     * @param cacheManager  the CacheManager that's creating the TarantoolCache
     * @param cacheName     the name of the Cache
     * @param classLoader   the ClassLoader the TarantoolCache will use for loading classes
     * @param configuration the Configuration of the Cache
     */
    TarantoolCache(TarantoolCacheManager cacheManager,
                   String cacheName,
                   ClassLoader classLoader,
                   Configuration<K, V> configuration) {

        if (!configuration.isStoreByValue()) {
            throw new UnsupportedOperationException("StoreByReference mode is not supported");
        }

        this.cacheManager = cacheManager;
        this.cacheName = cacheName;
        this.spaceName = cacheName.replaceAll("[^a-zA-Z0-9]", "_");

        //we make a copy of the configuration here so that the provided one
        //may be changed and or used independently for other caches.  we do this
        //as we don't know if the provided configuration is mutable
        if (configuration instanceof CompleteConfiguration) {
            //support use of CompleteConfiguration
            this.configuration = new MutableConfiguration<>((MutableConfiguration<K, V>) configuration);
        } else {
            //support use of Basic Configuration
            MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
            mutableConfiguration.setStoreByValue(configuration.isStoreByValue());
            mutableConfiguration.setTypes(configuration.getKeyType(), configuration.getValueType());
            this.configuration = mutableConfiguration;
        }

        if (this.configuration.getCacheLoaderFactory() != null) {
            cacheLoader = (CacheLoader<K, V>) this.configuration.getCacheLoaderFactory().create();
        }
        if (this.configuration.getCacheWriterFactory() != null) {
            cacheWriter = (CacheWriter<K, V>) this.configuration.getCacheWriterFactory().create();
        }

        expiryPolicy = this.configuration.getExpiryPolicyFactory().create();

        //establish all of the listeners
        for (CacheEntryListenerConfiguration<K, V> listenerConfiguration :
                this.configuration.getCacheEntryListenerConfigurations()) {
            createAndAddListener(listenerConfiguration);
        }

        final boolean spaceExists = checkSpaceExists();
        if (spaceExists) {
            this.spaceId = getSpaceId();
        } else {
            this.spaceId = createSpace();
        }

        log.info("cache initialized: spaceName={}, spaceId={}", spaceName, spaceId);

        cacheMXBean = new TarantoolCacheMXBean<>(this);
        statistics = new TarantoolCacheStatisticsMXBean();
        //It's important that we set the status BEFORE we let management, statistics and listeners know about the cache.
        isClosed = false;

        if (this.configuration.isManagementEnabled()) {
            setManagementEnabled(true);
        }

        if (this.configuration.isStatisticsEnabled()) {
            setStatisticsEnabled(true);
        }
    }

    private boolean checkSpaceExists() {
        String command = "box.space." + spaceName + " ~= nil";
        List<?> response = cacheManager.execute(command);
        if (response.isEmpty() || !Boolean.class.isInstance(response.get(0))) {
            throw new CacheException("Invalid response on command '" + command + "': expected boolean got " + response);
        }
        return (Boolean) response.get(0);
    }

    private int getSpaceId() {
        String command = "box.space." + spaceName + ".id";
        List<?> response = cacheManager.execute(command);
        if (response.isEmpty() || !Integer.class.isInstance(response.get(0))) {
            throw new CacheException("Invalid response on command '" + command + "': expected integer got " + response);
        }
        return (Integer) response.get(0);
    }

    private int createSpace() {
        final String command = "box.schema.space.create('" + spaceName + "').id";
        final List<?> response = cacheManager.execute(command);
        if (response.isEmpty() || !Integer.class.isInstance(response.get(0))) {
            throw new CacheException("Invalid response on command '" + command + "': expected integer got " + response);
        }
        final Integer spaceId = (Integer) response.get(0);

        try {
            final Map<String, String> fields = new HashMap<>();
            fields.put("name", "key");
            fields.put("type", "string");
            cacheManager.call("box.space." + spaceName + ":format", singletonList(fields));
        } catch (Throwable t) {
            cacheManager.call("box.space." + this.spaceName + ":drop");
            throw new CacheException("Cannot format space " + this.spaceName, t);
        }

        try {
            final Map<String, Object> index = new HashMap<>();
            index.put("parts", singletonList("key"));
            index.put("type", "hash");
            cacheManager.call("box.space." + spaceName + ":create_index", "primary", index);
        } catch (Throwable t) {
            cacheManager.call("box.space." + this.spaceName + ":drop");
            throw new CacheException("Cannot create primary index in space " + this.spaceName, t);
        }

        return spaceId;
    }

    private CachedValue cachedValueFromTuple(Object tuple) {
        if (tuple instanceof List) {
            List<?> items = (List<?>) tuple;
            /* Check here field count, it must be 4 (key, value, time, expire) */
            if (items.size() >= 4) {
                long creationTime = 0;
                if (Long.class.isInstance(items.get(2))) {
                    creationTime = (Long) items.get(2);
                }
                long expiryTime = -1;
                if (Long.class.isInstance(items.get(3))) {
                    expiryTime = (Long) items.get(3);
                }
                return new CachedValue(items.get(1), creationTime, expiryTime);
            }
        }
        return null;
    }

    private CachedValue getCachedValue(K key) {
        List<?> tuples = cacheManager.select(spaceId, singletonList(key.toString()), TarantoolIterator.ITER_EQ, 1);
        for (Object tuple : tuples) {
            CachedValue cachedValue = cachedValueFromTuple(tuple);
            if (cachedValue != null) {
                return cachedValue;
            }
        }
        return null;
    }

    private void putCachedValue(K key, CachedValue cachedValue) {
        List<?> tuple = Arrays.asList(key.toString(), cachedValue.get(), cachedValue.getCreationTime(), cachedValue.getExpiryTime());
        List<?> op1 = Arrays.asList("=", 1, cachedValue.get());
        List<?> op2 = Arrays.asList("=", 3, cachedValue.getExpiryTime());
        cacheManager.upsert(spaceId, singletonList(key.toString()), tuple, op1, op2);
    }

    private void updateCachedValue(K key, V value, long now) {
        Duration duration = null;
        try {
            duration = expiryPolicy.getExpiryForUpdate();
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }

        if (duration != null) {
            long expiryTime = duration.getAdjustedTime(now);
            List<?> op1 = Arrays.asList("=", 1, value);
            List<?> op2 = Arrays.asList("=", 3, expiryTime);
            cacheManager.update(spaceId, singletonList(key.toString()), op1, op2);
        } else {
            List<?> op = Arrays.asList("=", 1, value);
            cacheManager.update(spaceId, singletonList(key.toString()), op);
        }
    }

    private void updateAccessTime(K key, long now) {
        try {
            Duration duration = expiryPolicy.getExpiryForAccess();
            if (duration != null) {
                long expiryTime = duration.getAdjustedTime(now);
                List<?> op = Arrays.asList("=", 3, expiryTime);
                cacheManager.update(spaceId, singletonList(key.toString()), op);
            }
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
        }
    }

    private K getKeyFromInternal(Object internalKey) {
        Class<K> keyType = configuration.getKeyType();
        if (Integer.class.isAssignableFrom(keyType)) {
            try {
                Integer key = Integer.valueOf((String) internalKey);
                return keyType.cast(key);
            } catch (Exception e) {
                throw e;
            }
        } else if (Long.class.isAssignableFrom(keyType)) {
            try {
                Long key = Long.valueOf((String) internalKey);
                return keyType.cast(key);
            } catch (Exception e) {
                throw e;
            }
        } else {
            try {
                return keyType.cast(internalKey);
            } catch (Exception e) {
                throw e;
            }
        }
    }

    //todo concurrency
    private void createAndAddListener(CacheEntryListenerConfiguration<K, V> listenerConfiguration) {

    }

    //todo concurrency
    private void removeListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {

    }

    /**
     * Requests a {@link Runnable} to be performed.
     *
     * @param task the {@link Runnable} to be performed
     */
    protected void submit(Runnable task) {
        executorService.submit(task);
    }

    /**
     * The default Duration to use when a Duration can't be determined.
     *
     * @return the default Duration
     */
    protected Duration getDefaultDuration() {
        return Duration.ETERNAL;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return cacheName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        if (!isClosed()) {
            //ensure that any further access to this Cache will raise an
            // IllegalStateException
            isClosed = true;

            //ensure that the cache may no longer be accessed via the CacheManager
            cacheManager.releaseCache(cacheName);

            //disable statistics and management
            setStatisticsEnabled(false);
            setManagementEnabled(false);

            //close the configured CacheLoader
            if (cacheLoader instanceof Closeable) {
                try {
                    ((Closeable) cacheLoader).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing CacheLoader", e);
                }
            }

            //close the configured CacheWriter
            if (cacheWriter instanceof Closeable) {
                try {
                    ((Closeable) cacheWriter).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing CacheWriter", e);
                }
            }

            //close the configured ExpiryPolicy
            if (expiryPolicy instanceof Closeable) {
                try {
                    ((Closeable) expiryPolicy).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing ExpiryPolicy", e);
                }
            }

            //attempt to shutdown (and wait for the cache to shutdown)
            executorService.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executorService.awaitTermination(EXECUTOR_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
                  executorService.shutdownNow(); // Cancel currently executing tasks
                  // Wait a while for tasks to respond to being cancelled
                  if (!executorService.awaitTermination(EXECUTOR_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
                      log.error("executorService did not terminate");
                  }
                }
            } catch (InterruptedException e) {
                // (Re-)Cancel if current thread also interrupted
                executorService.shutdownNow();
                throw new CacheException(e);
            }

            //drop all entries from the cache
            cacheManager.call("box.space." + this.spaceName + ":truncate");
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
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(configuration)) {
            return clazz.cast(configuration);
        }
        throw new IllegalArgumentException("The configuration class " + clazz +
                " is not supported by this implementation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }

        return getValue(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        ensureOpen();
        // will throw NPE if keys=null
        HashMap<K, V> map = new HashMap<>(keys.size());

        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException("keys contains a null");
            }
            V value = getValue(key);
            if (value != null) {
                map.put(key, value);
            }
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException();
        }

        long now = System.currentTimeMillis();
        List<?> tuples = cacheManager.select(spaceId, singletonList(key.toString()), TarantoolIterator.ITER_EQ, 1);
        for (Object tuple : tuples) {
            if (tuple instanceof List) {
                List<?> items = (List<?>) tuple;
                /* Check here field count, it must be 4 (key, value, time, expire) */
                if (items.size() >= 4) {
                    long expiryTime = -1;
                    if (Long.class.isInstance(items.get(3))) {
                        expiryTime = (Long) items.get(3);
                    }
                    return !CachedValue.isExpired(now, expiryTime);
                }
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadAll(final Set<? extends K> keys,
                        final boolean replaceExistingValues,
                        final CompletionListener completionListener) {
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException("keys");
        }

        if (cacheLoader == null) {
            if (completionListener != null) {
                completionListener.onCompletion();
            }
        } else {
            for (K key : keys) {
                if (key == null) {
                    throw new NullPointerException("keys contains a null");
                }
            }

            submit(() -> {
                try {
                    ArrayList<K> keysToLoad = new ArrayList<K>();
                    for (K key : keys) {
                        if (replaceExistingValues || !containsKey(key)) {
                            keysToLoad.add(key);
                        }
                    }

                    Map<? extends K, ? extends V> loaded;
                    try {
                        loaded = cacheLoader.loadAll(keysToLoad);
                    } catch (Exception e) {
                        if (!(e instanceof CacheLoaderException)) {
                            throw new CacheLoaderException("Exception in CacheLoader", e);
                        } else {
                            throw e;
                        }
                    }

                    for (K key : keysToLoad) {
                        if (loaded.get(key) == null) {
                            loaded.remove(key);
                        }
                    }

                    putAll(loaded, replaceExistingValues, false);

                    if (completionListener != null) {
                        completionListener.onCompletion();
                    }
                } catch (Exception e) {
                    if (completionListener != null) {
                        completionListener.onException(e);
                    }
                }
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(K key, V value) {
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        int putCount = 0;
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);

        try {
            long now = System.currentTimeMillis();
            CachedValue cachedValue = getCachedValue(key);

            boolean isOldEntryExpired = cachedValue != null && cachedValue.isExpiredAt(now);

            if (isOldEntryExpired) {
                V expiredValue = cachedValue.get();
                processExpiries(key, expiredValue);
            }

            if (cachedValue == null || isOldEntryExpired) {

                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value);
                Duration duration;
                try {
                    duration = expiryPolicy.getExpiryForCreation();
                } catch (Throwable t) {
                    duration = getDefaultDuration();
                }
                long expiryTime = duration.getAdjustedTime(now);

                cachedValue = new CachedValue(value, now, expiryTime);

                //todo: writes should not happen on a new expired entry
                writeCacheEntry(entry);


                // check that new entry is not already expired, in which case it should
                // not be added to the cache or listeners called or writers called.
                if (cachedValue.isExpiredAt(now)) {
                    V expiredValue = cachedValue.get();
                    processExpiries(key, expiredValue);
                } else {
                    putCachedValue(key, cachedValue);
                    putCount++;
                }

            } else {

                V oldValue = cachedValue.get();
                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value, oldValue);

                writeCacheEntry(entry);
                updateCachedValue(key, value, now);
                putCount++;
            }

        } catch (Exception e) {
            throw e;
        }
        if (statisticsEnabled() && putCount > 0) {
            statistics.increaseCachePuts(putCount);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }
    }

    private void checkTypesAgainstConfiguredTypes(K key, V value) throws ClassCastException {
        Class<K> keyType = configuration.getKeyType();
        Class<V> valueType = configuration.getValueType();
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
        if (Object.class != valueType) {
            //means type checks required
            if (!valueType.isAssignableFrom(value.getClass())) {
                throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
            }
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        long now = System.currentTimeMillis();

        V result = null;
        int putCount = 0;
        try {
            CachedValue cachedValue = getCachedValue(key);

            boolean isExpired = cachedValue != null && cachedValue.isExpiredAt(now);
            if (cachedValue == null || isExpired) {
                result = null;

                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value);
                writeCacheEntry(entry);

                if (isExpired) {
                    V expiredValue = cachedValue.get();
                    processExpiries(key, expiredValue);
                }

                Duration duration;
                try {
                    duration = expiryPolicy.getExpiryForCreation();
                } catch (Throwable t) {
                    duration = getDefaultDuration();
                }
                long expiryTime = duration.getAdjustedTime(now);

                cachedValue = new CachedValue(value, now, expiryTime);
                if (cachedValue.isExpiredAt(now)) {
                    processExpiries(key, value);
                } else {
                    putCachedValue(key, cachedValue);
                    putCount++;
                }

            } else {
                V oldValue = cachedValue.getInternalValue(now);
                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value, oldValue);
                writeCacheEntry(entry);
                updateCachedValue(key, value, now);
                putCount++;
                result = oldValue;
            }
        } catch (Exception e) {
            throw e;
        }
        if (statisticsEnabled()) {
            if (result == null) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);

            if (putCount > 0) {
                statistics.increaseCachePuts(putCount);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        putAll(map, true);
    }

    /**
     */
    public void putAll(Map<? extends K, ? extends V> map,
                       boolean replaceExistingValues) {
        putAll(map, replaceExistingValues, true);
    }

    /**
     * A implementation of PutAll that allows optional replacement of existing
     * values and optionally writing values when Write Through is configured.
     *
     * @param map                   the Map of entries to put
     * @param replaceExistingValues should existing values be replaced by those in
     *                              the map?
     * @param useWriteThrough       should write-through be used if it is configured
     */
    public void putAll(Map<? extends K, ? extends V> map,
                       final boolean replaceExistingValues,
                       boolean useWriteThrough) {
        ensureOpen();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();
        int putCount = 0;

        CacheWriterException exception = null;

        try {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter !=
                    null && useWriteThrough;

            ArrayList<Cache.Entry<? extends K, ? extends V>> entriesToWrite = new
                    ArrayList<>();
            HashSet<K> keysToPut = new HashSet<>();
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                if (value == null) {
                    throw new NullPointerException("key " + key + " has a null value");
                }

                keysToPut.add(key);

                if (isWriteThrough) {
                    entriesToWrite.add(new TarantoolEntry<>(key, value));
                }
            }

            //write the entries
            if (isWriteThrough) {
                try {
                    cacheWriter.writeAll(entriesToWrite);
                } catch (Exception e) {
                    if (!(e instanceof CacheWriterException)) {
                        exception = new CacheWriterException("Exception during write", e);
                    }
                }

                for (Entry<?, ?> entry : entriesToWrite) {
                    keysToPut.remove(entry.getKey());
                }
            }

            //perform the put
            for (K key : keysToPut) {
                V value = map.get(key);

                CachedValue cachedValue = getCachedValue(key);

                boolean isExpired = cachedValue != null && cachedValue.isExpiredAt(now);
                if (cachedValue == null || isExpired) {

                    if (isExpired) {
                        V expiredValue = cachedValue.get();
                        processExpiries(key, expiredValue);
                    }

                    Duration duration;
                    try {
                        duration = expiryPolicy.getExpiryForCreation();
                    } catch (Throwable t) {
                        duration = getDefaultDuration();
                    }
                    long expiryTime = duration.getAdjustedTime(now);

                    cachedValue = new CachedValue(value, now, expiryTime);
                    if (cachedValue.isExpiredAt(now)) {
                        processExpiries(key, value);
                    } else {
                        putCachedValue(key, cachedValue);

                        // this method called from loadAll when useWriteThrough is false. do
                        // not count loads as puts per statistics
                        // table in specification.
                        if (useWriteThrough) {
                            putCount++;
                        }
                    }
                } else if (replaceExistingValues) {
                    updateCachedValue(key, value, now);
                    // do not count loadAll calls as puts. useWriteThrough is false when
                    // called from loadAll.
                    if (useWriteThrough) {
                        putCount++;
                    }

                }
            }
        } catch (Exception e) {
            throw e;
        }

        if (statisticsEnabled() && putCount > 0) {
            statistics.increaseCachePuts(putCount);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean putIfAbsent(K key, V value) {
        ensureOpen();
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);

        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();

        boolean result = false;
        try {
            CachedValue cachedValue = getCachedValue(key);

            boolean isExpired = cachedValue != null && cachedValue.isExpiredAt(now);
            if (cachedValue == null || cachedValue.isExpiredAt(now)) {

                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value);
                writeCacheEntry(entry);

                if (isExpired) {
                    V expiredValue = cachedValue.get();
                    processExpiries(key, expiredValue);
                }

                Duration duration;
                try {
                    duration = expiryPolicy.getExpiryForCreation();
                } catch (Throwable t) {
                    duration = getDefaultDuration();
                }
                long expiryTime = duration.getAdjustedTime(now);

                cachedValue = new CachedValue(value, now, expiryTime);
                if (cachedValue.isExpiredAt(now)) {
                    processExpiries(key, value);

                    // no expiry event for created entry that expires before put in cache.
                    // do not put entry in cache.
                    result = false;
                } else {
                    putCachedValue(key, cachedValue);
                    result = true;
                }
            } else {
                result = false;
            }

        } catch (Exception e) {
            throw e;
        }

        if (statisticsEnabled()) {
            if (result) {
                //this means that there was no key in the Cache and the put succeeded
                statistics.increaseCachePuts(1);
                statistics.increaseCacheMisses(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                //this means that there was a key in the Cache and the put did not succeed
                statistics.increaseCacheHits(1);
            }
        }
        return result;
    }

    private void processExpiries(K key,
                                 V expiredValue) {
        cacheManager.delete(spaceId, singletonList(key.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();
        deleteCacheEntry(key);
        List<?> tuplesDeleted = cacheManager.delete(spaceId, singletonList(key.toString()));
        boolean result = false;
        for (Object tuple : tuplesDeleted) {
            if (tuple instanceof List) {
                List<?> items = (List<?>) tuple;
                /* Check here field count, it must be 4 (key, value, time, expire) */
                if (items.size() >= 4) {
                    long expiryTime = -1;
                    if (Long.class.isInstance(items.get(3))) {
                        expiryTime = (Long) items.get(3);
                    }
                    result = !CachedValue.isExpired(now, expiryTime);
                    break;
                }
            }
        }
        if (result && statisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key, V oldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long hitCount = 0;

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result;
        CachedValue cachedValue = getCachedValue(key);
        if (cachedValue == null || cachedValue.isExpiredAt(now)) {
            result = false;
        } else {
            hitCount++;

            if (cachedValue.get().equals(oldValue)) {
                deleteCacheEntry(key);
                List<?> deletedTuples = cacheManager.delete(spaceId, singletonList(key.toString()));
                result = deletedTuples.size() > 0;
            } else {
                updateAccessTime(key, now);
                result = false;
            }
        }
        if (statisticsEnabled()) {
            if (result) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getAndRemove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);
        V result = null;
        List<?> deletedTuples = cacheManager.delete(spaceId, singletonList(key.toString()));
        for (Object tuple : deletedTuples) {
            CachedValue cachedValue = cachedValueFromTuple(tuple);
            if (cachedValue != null) {
                result = cachedValue.isExpiredAt(now) ? null : cachedValue.get();
                break;
            }
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (newValue == null) {
            throw new NullPointerException("null newValue specified for key " + key);
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        long hitCount = 0;

        boolean result = false;
        try {
            CachedValue cachedValue = getCachedValue(key);
            if (cachedValue == null || cachedValue.isExpiredAt(now)) {
                result = false;
            } else {
                hitCount++;

                if (cachedValue.get().equals(oldValue)) {

                    TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, newValue, oldValue);
                    writeCacheEntry(entry);
                    updateCachedValue(key, newValue, now);
                    result = true;
                } else {
                    updateAccessTime(key, now);
                    result = false;
                }
            }
        } catch (Exception e) {
            throw e;
        }
        if (statisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result = false;
        try {
            CachedValue cachedValue = getCachedValue(key);
            if (cachedValue == null || cachedValue.isExpiredAt(now)) {
                result = false;
            } else {
                V oldValue = cachedValue.get();

                TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value, oldValue);
                writeCacheEntry(entry);
                updateCachedValue(key, value, now);
                result = true;
            }
        } catch (Exception e) {
            throw e;
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.increaseCacheHits(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getAndReplace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        V result;

        CachedValue cachedValue = getCachedValue(key);
        if (cachedValue == null || cachedValue.isExpiredAt(now)) {
            result = null;
        } else {
            V oldValue = cachedValue.getInternalValue(now);
            TarantoolEntry<K, V> entry = new TarantoolEntry<>(key, value, oldValue);
            writeCacheEntry(entry);
            updateCachedValue(key, value, now);
            result = oldValue;
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        CacheException exception = null;
        if (keys.size() > 0) {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter != null;
            HashSet<Object> deletedKeys = new HashSet<>();

            //call write-through on deleted entries
            if (isWriteThrough) {
                HashSet<K> cacheWriterKeys = new HashSet<>(keys);
                try {
                    cacheWriter.deleteAll(cacheWriterKeys);
                } catch (Exception e) {
                    if (!(e instanceof CacheWriterException)) {
                        exception = new CacheWriterException("Exception during write", e);
                    }
                }

                //At this point, cacheWriterKeys will contain only those that were _not_ written
                //Now delete only those that the writer deleted
                for (K key : keys) {
                    //only delete those keys that the writer deleted. per CacheWriter spec.
                    if (!cacheWriterKeys.contains(key)) {
                        List<?> deletedTuples = cacheManager.delete(spaceId, singletonList(key.toString()));
                        if (deletedTuples.size() > 0) {
                            deletedKeys.add(key);
                        }
                    }
                }
            } else {

                for (K key : keys) {
                    List<?> deletedTuples = cacheManager.delete(spaceId, singletonList(key.toString()));
                    if (deletedTuples.size() > 0) {
                        deletedKeys.add(key);
                    }
                }
            }

            //Update stats
            if (statisticsEnabled()) {
                statistics.increaseCacheRemovals(deletedKeys.size());
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll() {
        ensureOpen();
        int cacheRemovals = 0;
        long now = System.currentTimeMillis();
        CacheException exception = null;
        HashSet<K> allExpiredKeys = new HashSet<>();
        HashSet<K> allNonExpiredKeys = new HashSet<>();
        HashSet<K> keysToDelete = new HashSet<>();

        try {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter != null;
            int limit = MAX_ROWS_PER_ITER_ALL;
            List<?> tuples = cacheManager.select(spaceId, Collections.emptyList(), TarantoolIterator.ITER_ALL, limit);
            for (Object tuple : tuples) {
                if (tuple instanceof List) {
                    List<?> items = (List<?>) tuple;
                    /* Check here field count, it must be 4 (key, value, time, expire) */
                    if (items.size() >= 4) {
                        K key = getKeyFromInternal(items.get(0));
                        long expiryTime = -1;
                        if (Long.class.isInstance(items.get(3))) {
                            expiryTime = (Long) items.get(3);
                        }
                        if (CachedValue.isExpired(now, expiryTime)) {
                            allExpiredKeys.add(key);
                        } else {
                            allNonExpiredKeys.add(key);
                        }
                        if (isWriteThrough) {
                            keysToDelete.add(key);
                        }
                    }
                }
            }

            //delete the entries (when there are some)
            if (isWriteThrough && keysToDelete.size() > 0) {
                try {
                    cacheWriter.deleteAll(keysToDelete);
                } catch (Exception e) {
                    if (!(e instanceof CacheWriterException)) {
                        exception = new CacheWriterException("Exception during write", e);
                    }
                }
            }

            //remove the deleted keys that were successfully deleted from the set (only non-expired)
            for (K key : allNonExpiredKeys) {
                if (!keysToDelete.contains(key)) {
                    cacheManager.delete(spaceId, singletonList(key.toString()));
                    cacheRemovals++;
                }
            }

            //remove the deleted keys that were successfully deleted from the set (only expired)
            for (K key : allExpiredKeys) {
                if (!keysToDelete.contains(key)) {
                    cacheManager.delete(spaceId, singletonList(key.toString()));
                }
            }
        } catch (Exception e) {
            throw e;
        }

        if (statisticsEnabled()) {
            statistics.increaseCacheRemovals(cacheRemovals);
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        ensureOpen();
        cacheManager.call("box.space." + this.spaceName + ":truncate");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invoke(K key, javax.cache.processor.EntryProcessor<K, V,
            T> entryProcessor, Object... arguments) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException();
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }

        long start = statisticsEnabled() ? System.nanoTime() : 0;


        T result;
        try {
            long now = System.currentTimeMillis();

            CachedValue cachedValue = getCachedValue(key);
            boolean isExpired = cachedValue != null && cachedValue.isExpiredAt(now);

            if (isExpired) {
                V expiredValue = cachedValue.get();
                processExpiries(key, expiredValue);
            }
            if (statisticsEnabled()) {
                if (cachedValue == null || isExpired) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }

                statistics.addGetTimeNano(System.nanoTime() - start);
            }

            //restart start as fetch finished
            start = statisticsEnabled() ? System.nanoTime() : 0;

            EntryProcessorEntry<K, V> entry = new EntryProcessorEntry<>(key,
                    cachedValue, now, configuration.isReadThrough() ? cacheLoader : null);
            try {
                result = entryProcessor.process(entry, arguments);
            } catch (CacheException e) {
                throw e;
            } catch (Exception e) {
                throw new EntryProcessorException(e);
            }

            Duration duration;
            long expiryTime;
            switch (entry.getOperation()) {
                case NONE:
                    break;

                case ACCESS:
                    updateAccessTime(key, now);
                    break;

                case CREATE:
                case LOAD:
                    TarantoolEntry<K, V> e = new TarantoolEntry<>(key, entry.getValue());

                    if (entry.getOperation() == MutableEntryOperation.CREATE) {
                        writeCacheEntry(e);
                    }

                    try {
                        duration = expiryPolicy.getExpiryForCreation();
                    } catch (Throwable t) {
                        duration = getDefaultDuration();
                    }
                    expiryTime = duration.getAdjustedTime(now);
                    cachedValue = new CachedValue(entry.getValue(), now, expiryTime);
                    if (cachedValue.isExpiredAt(now)) {
                        V previousValue = cachedValue.get();
                        processExpiries(key, previousValue);
                    } else {
                        putCachedValue(key, cachedValue);

                        // do not count LOAD as a put for cache statistics.
                        if (statisticsEnabled() && entry.getOperation() ==
                                MutableEntryOperation.CREATE) {
                            statistics.increaseCachePuts(1);
                            statistics.addPutTimeNano(System.nanoTime() - start);
                        }
                    }

                    break;

                case UPDATE:
                    V oldValue = cachedValue.get();

                    e = new TarantoolEntry<>(key, entry.getValue(), oldValue);
                    writeCacheEntry(e);

                    updateCachedValue(key, entry.getValue(), now);
                    if (statisticsEnabled()) {
                        statistics.increaseCachePuts(1);
                        statistics.addPutTimeNano(System.nanoTime() - start);
                    }

                    break;

                case REMOVE:
                    deleteCacheEntry(key);
                    cacheManager.delete(spaceId, singletonList(key.toString()));

                    if (statisticsEnabled()) {
                        statistics.increaseCacheRemovals(1);
                        statistics.addRemoveTimeNano(System.nanoTime() - start);
                    }

                    break;

                default:
                    break;
            }


        } catch (Exception e) {
            throw e;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
                                                         EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException();
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }

        HashMap<K, EntryProcessorResult<T>> map = new HashMap<>();
        for (K key : keys) {
            TarantoolEntryProcessorResult<T> result;
            try {
                T t = invoke(key, entryProcessor, arguments);
                result = t == null ? null : new TarantoolEntryProcessorResult<>(t);
            } catch (Exception e) {
                result = new TarantoolEntryProcessorResult<>(e);
            }
            if (result != null) {
                map.put(key, result);
            }
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        long now = System.currentTimeMillis();
        return new EntryIterator(now);
    }

    /**
     * @return the management bean
     */
    public CacheMXBean getCacheMXBean() {
        return cacheMXBean;
    }


    /**
     * @return the management bean
     */
    public CacheStatisticsMXBean getCacheStatisticsMXBean() {
        return statistics;
    }


    /**
     * Sets statistics
     */
    public void setStatisticsEnabled(boolean enabled) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Statistics);
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, Statistics);
        }
        configuration.setStatisticsEnabled(enabled);
    }


    /**
     * Sets management enablement
     *
     * @param enabled true if management should be enabled
     */
    public void setManagementEnabled(boolean enabled) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Configuration);
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, Configuration);
        }
        configuration.setManagementEnabled(enabled);
    }

    private void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. " +
                    "The cache closed");
        }
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> cls) {
        if (cls.isAssignableFrom(((Object) this).getClass())) {
            return cls.cast(this);
        }

        throw new IllegalArgumentException("Unwrapping to " + cls + " is not " +
                "supported by this implementation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        configuration.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
        createAndAddListener(cacheEntryListenerConfiguration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K,
            V> cacheEntryListenerConfiguration) {
        configuration.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
        removeListener(cacheEntryListenerConfiguration);
    }


    private boolean statisticsEnabled() {
        return configuration.isStatisticsEnabled();
    }

    /**
     * Writes the Cache Entry to the configured CacheWriter.  Does nothing if
     * write-through is not configured.
     *
     * @param entry the Cache Entry to write
     */
    private void writeCacheEntry(TarantoolEntry<K, V> entry) {
        if (configuration.isWriteThrough()) {
            try {
                cacheWriter.write(entry);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter", e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Deletes the Cache Entry using the configured CacheWriter.  Does nothing
     * if write-through is not configured.
     *
     * @param key
     */
    private void deleteCacheEntry(K key) {
        if (configuration.isWriteThrough()) {
            try {
                cacheWriter.delete(key);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter", e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Gets the value for the specified key from the underlying cache, including
     * attempting to load it if a CacheLoader is configured (with read-through).
     * <p>
     * Any events that need to be raised are added to the specified dispatcher.
     * </p>
     *
     * @param key the key of the entry to get from the cache
     * @return the value loaded
     */
    private V getValue(K key) {
        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        CachedValue cachedValue = null;
        V value = null;
        try {
            cachedValue = getCachedValue(key);

            boolean isExpired = cachedValue != null && cachedValue.isExpiredAt(now);

            if (cachedValue == null || isExpired) {

                V expiredValue = isExpired ? cachedValue.get() : null;

                if (isExpired) {
                    processExpiries(key, expiredValue);
                }

                if (statisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }

                if (configuration.isReadThrough() && cacheLoader != null) {
                    try {
                        value = cacheLoader.load(key);
                    } catch (Exception e) {
                        if (!(e instanceof CacheLoaderException)) {
                            throw new CacheLoaderException("Exception in CacheLoader", e);
                        } else {
                            throw e;
                        }
                    }
                }

                if (value == null) {
                    return null;
                }

                Duration duration;
                try {
                    duration = expiryPolicy.getExpiryForCreation();
                } catch (Throwable t) {
                    duration = getDefaultDuration();
                }
                long expiryTime = duration.getAdjustedTime(now);
                cachedValue = new CachedValue(value, now, expiryTime);
                if (cachedValue.isExpiredAt(now)) {
                    return null;
                } else {
                    putCachedValue(key, cachedValue);

                    // do not consider a load as a put for cache statistics.
                }
            } else {
                value = cachedValue.getInternalValue(now);
                updateAccessTime(key, now);

                if (statisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                }
            }

        } finally {
            if (statisticsEnabled()) {
                statistics.addGetTimeNano(System.nanoTime() - start);
            }
        }
        return value;
    }

    /**
     * An {@link Iterator} over Cache {@link Entry}s that lazily converts
     * from internal value representation to natural value representation on
     * demand.
     */
    private final class EntryIterator implements Iterator<Entry<K, V>> {

      /**
       * The next available non-expired cache entry to return.
       */
      private TarantoolEntry<K, V> nextEntry;

      /**
       * The last returned cache entry (so we can allow for removal)
       */
      private TarantoolEntry<K, V> lastEntry;

      /**
       * The time the iteration commenced.  We use this to determine what
       * Cache Entries in the underlying iterator are expired.
       */
      private long now;

      /**
       * Constructs an {@link EntryIterator}.
       *
       * @param now      the time the iterator will use to test for expiry
       */
      private EntryIterator(long now) {
        this.nextEntry = null;
        this.lastEntry = null;
        this.now = now;
      }

      private void fetch() {
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        while (nextEntry == null) {
            K key = null;
            List<?> keys;
            TarantoolIterator iterator;
            if (lastEntry != null) {
                /* Adjust iterator for fetching next tuple from Tarantool's space */
                /* Tarantool supports different type of iteration (See TarantoolIterator), */
                /* but not every index (HASH, TREE, ...) supports these types. */
                /* See https://tarantool.io/en/doc/2.0/book/box/data_model/ */
                keys = singletonList(lastEntry.getKey().toString());
                iterator = TarantoolIterator.ITER_GT;
            } else {
                keys = Collections.emptyList();
                iterator = TarantoolIterator.ITER_ALL;
            }
            CachedValue cachedValue = null;
            try {
                List<?> tuples = cacheManager.select(spaceId, keys, iterator, MAX_ROWS_PER_ITER_ONE);
                for (Object tuple : tuples) {
                    if (tuple instanceof List) {
                        List<?> items = (List<?>) tuple;
                        cachedValue = cachedValueFromTuple(tuple);
                        /* Check here field count, it must be 4 (key, value, time, expire) */
                        if (items.size() >= 4) {
                            key = getKeyFromInternal(items.get(0));
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                throw e;
            }

            if (key == null) {
                break;
            }

            if (cachedValue != null) {
                lastEntry = new TarantoolEntry<>(key, cachedValue.get());
            }

            try {
              if (!cachedValue.isExpiredAt(now)) {
                V value = cachedValue.getInternalValue(now);
                nextEntry = new TarantoolEntry<>(key, value);
                updateAccessTime(key, now);
              }
            } finally {
              if (statisticsEnabled() && nextEntry != null) {
                statistics.increaseCacheHits(1);
                statistics.addGetTimeNano(System.nanoTime() - start);
              }
            }
          }
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public boolean hasNext() {
          if (nextEntry == null) {
            fetch();
          }
          return nextEntry != null;
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public Entry<K, V> next() {
          if (hasNext()) {
              //remember the lastEntry (so that we call allow for removal)
              lastEntry = nextEntry;

              //reset nextEntry to force fetching the next available entry
              nextEntry = null;

              return lastEntry;
            } else {
              throw new NoSuchElementException();
            }
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public void remove() {
          int cacheRemovals = 0;
          if (lastEntry == null) {
            throw new IllegalStateException("Must progress to the next entry to remove");
          } else {
            long start = statisticsEnabled() ? System.nanoTime() : 0;
            try {
              deleteCacheEntry(lastEntry.getKey());

              //NOTE: there is the possibility here that the entry the application
              // retrieved
              //may have been replaced / expired or already removed since it
              // retrieved it.

              //we simply don't care here as multiple-threads are ok to remove and see
              //such side-effects
              List<?> deletedTuples = cacheManager.delete(spaceId, singletonList(lastEntry.getKey().toString()));
              if (deletedTuples.size() > 0) {
                  cacheRemovals++;
              }
            } finally {
              //reset lastEntry (we can't attempt to remove it again)
              lastEntry = null;
              if (statisticsEnabled() && cacheRemovals > 0) {
                statistics.increaseCacheRemovals(cacheRemovals);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
              }
            }
          }
        }
    }

}
