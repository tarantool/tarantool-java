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

/**
 * Represents the internal Cache Entry Value with in an {@link TarantoolCache}.
 * <p>
 * The actual value passed to the Cache is represented in an internal format.
 * </p>
 * {@link CachedValue}s additionally store and provide meta information about
 * Cache Entry Values, including information for dealing with expiry.
 *
 * @author Brian Oliver
 * @author Evgeniy Zaikin
 */
public class CachedValue {

  /**
   * The internal representation of the Cache Entry value.
   */
  private Object internalValue;

  /**
   * The time (since the Epoch) in milliseconds since the internal value was created.
   */
  private long creationTime;

  /**
   * The time (since the Epoch) in milliseconds since the internal value was
   * last accessed.
   */
  private long accessTime;

  /**
   * The number of times the interval value has been accessed.
   */
  private long accessCount;

  /**
   * The time (since the Epoch) in milliseconds since the internal value was
   * last modified.
   */
  private long modificationTime;

  /**
   * The number of times the internal value has been modified.
   */
  private long modificationCount;

  /**
   * The time (since the Epoch) in milliseconds when the Cache Entry associated
   * with this value should be considered expired.
   * <p>
   * A value of -1 indicates that the Cache Entry should never expire.
   * </p>
   */
  private long expiryTime;

  /**
   * Constructs an {@link CachedValue} with the creation, access and
   * modification times being the current time.
   *
   * @param internalValue the internal representation of the value
   * @param creationTime  the time when the cache entry was created
   * @param expiryTime    the time when the cache entry should expire
   */
  public CachedValue(Object internalValue, long creationTime, long expiryTime) {
    this.internalValue = internalValue;
    this.creationTime = creationTime;
    this.accessTime = creationTime;
    this.modificationTime = creationTime;
    this.expiryTime = expiryTime;
    this.accessCount = 0;
    this.modificationCount = 0;
  }

  /**
   * Gets the time (since the Epoch) in milliseconds since the internal value
   * was created.
   *
   * @return time in milliseconds (since the Epoch)
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * Gets the time (since the Epoch) in milliseconds since the internal value
   * was last accessed.
   *
   * @return time in milliseconds (since the Epoch)
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Gets the number of times the internal value has been accessed.
   *
   * @return the access count
   */
  public long getAccessCount() {
    return accessCount;
  }

  /**
   * Gets the time (since the Epoch) in milliseconds since the internal value
   * was last modified.
   *
   * @return time in milliseconds (since the Epoch)
   */
  public long getModificationTime() {
    return modificationTime;
  }

  /**
   * Gets the number of times the internal value has been modified (set)
   *
   * @return the modification count
   */
  public long getModificationCount() {
    return modificationCount;
  }

  /**
   * Gets the time (since the Epoch) in milliseconds when the Cache Entry
   * associated with this value should be considered expired.
   *
   * @return time in milliseconds (since the Epoch)
   */
  public long getExpiryTime() {
    return expiryTime;
  }

  /**
   * Sets the time (since the Epoch) in milliseconds when the Cache Entry
   * associated with this value should be considered expired.
   *
   * @param expiryTime time in milliseconds (since the Epoch)
   */
  public void setExpiryTime(long expiryTime) {
    this.expiryTime = expiryTime;
  }

  /**
   * Determines if the Cache Entry associated with this value would be expired
   * at the specified time
   *
   * @param now time in milliseconds (since the Epoch)
   * @return true if the value would be expired at the specified time
   */
  public boolean isExpiredAt(long now) {
    return expiryTime > -1 && expiryTime <= now;
  }

  /**
   * Determines if the Cache Entry associated with this value would be expired
   * at the specified time and expire time
   *
   * @param now time in milliseconds (since the Epoch)
   * @param expiryTime time in milliseconds (since the Epoch)
   * @return true if the value would be expired at the specified time
   */
  static public boolean isExpired(long now, long expiryTime) {
    return expiryTime > -1 && expiryTime <= now;
  }

  /**
   * Gets the internal value (without updating the access time).
   *
   * @return the internal value
   */
  @SuppressWarnings("unchecked")
  public <T> T get() {
    return (T)internalValue;
  }

  /**
   * Sets the internal value (without updating the modification time)
   *
   * @param internalValue the new internal value
   */
  public void set(Object internalValue) {
    this.internalValue = internalValue;
  }

  /**
   * Gets the internal value with the side-effect of updating the access time
   * to that which is specified and incrementing the access count.
   *
   * @param accessTime the time when the internal value was accessed
   * @return the internal value
   */
  @SuppressWarnings("unchecked")
  public <T> T getInternalValue(long accessTime) {
    this.accessTime = accessTime;
    this.accessCount++;
    return (T)internalValue;
  }

  /**
   * Sets the internal value with the additional side-effect of updating the
   * modification time to that which is specified and incrementing the
   * modification count.
   *
   * @param internalValue    the new internal value
   * @param modificationTime the time when the value was modified
   */
  public void setInternalValue(Object internalValue, long modificationTime) {
    this.modificationTime = modificationTime;
    this.internalValue = internalValue;
    this.modificationCount++;
  }
}
