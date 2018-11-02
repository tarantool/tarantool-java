<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Tarantool JCache Provider (JSR-107)

## Getting started

1. Add a dependency to your `pom.xml` file.

```xml
<dependency>
  <groupId>org.tarantool</groupId>
  <artifactId>connector</artifactId>
  <version>1.8.jcache-SNAPSHOT</version>
</dependency>
```

2. Configure `Hibernate` for use Tarantool JCache.

Edit application .properties file in your Spring project:
```
spring.jpa.properties.hibernate.cache.use_second_level_cache=true
spring.jpa.properties.hibernate.cache.use_structured_entries=true
spring.jpa.properties.hibernate.cache.region.factory_class=org.hibernate.cache.jcache.JCacheRegionFactory
spring.jpa.properties.hibernate.javax.cache.provider=org.tarantool.jsr107.TarantoolCachingProvider
spring.jpa.properties.hibernate.javax.cache.uri=http://guest@localhost:3301
```

... or configure via hibernate session factory XML file (hibernate.cfg.xml):
```xml
<property name="hibernate.cache.use_structured_entries">true</property>
<property name="hibernate.cache.use_second_level_cache">true</property>
<property name="hibernate.cache.region.factory_class">
	org.hibernate.cache.jcache.JCacheRegionFactory
</property>
<property name="hibernate.javax.cache.provider">
	org.tarantool.jsr107.TarantoolCachingProvider
</property>
<property name="hibernate.javax.cache.uri">
	http://guest@localhost:3301
</property>
```

3. Install and run Tarantool instance. Use default port (3301) on your localhost machine (see "hibernate.javax.cache.uri" property):
```lua
box.cfg{listen = 3301}
```

4. Grant user access to guest:
```lua
box.schema.user.grant('guest','read,write,execute,create,drop','universe')
```

For more information related with Tarantool see:
https://tarantool.io/en/doc/2.0/book/getting_started/using_docker/

