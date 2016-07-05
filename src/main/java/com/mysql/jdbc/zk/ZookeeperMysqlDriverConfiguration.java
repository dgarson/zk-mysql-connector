package com.mysql.jdbc.zk;

import com.google.common.base.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import javax.annotation.Nonnull;

/**
 * Properties necessary to configure Zookeeper for use with this MySQL driver extension. The JDBC URL should contain a
 * fully qualified class name for an implementation of this interface that has a public, zero-argument constructor.
 *
 * @author dgarson
 */
public interface ZookeeperMysqlDriverConfiguration {

    /**
     * Returns the zookeeper cluster hostnames
     */
    @Nonnull
    String getHostAddresses();

    /**
     * Performs any optional, additional configuration to the Zookeeper client that will be built and managed by Curator
     * and used by this driver.
     */
    void configureClient(@Nonnull CuratorFrameworkFactory.Builder clientBuilder);

    /**
     * Returns an optional maximum duration (in milliseconds) for a cache that can be used for the master hostname in
     * front of the Zookeeper-backed PathChildrenCache.
     */
    @Nonnull
    Optional<Long> getHostnameCacheDurationMillis();

    /**
     * Returns the current master host/port for a given database name, or an absent Optional if there is no known master
     * that can be used for that database.
     * @param databaseId the database identifier
     * @param zkHostNodeCache the Zookeeper-backed path-children node data cache based on a cache created via this
     *                        object's {@link #createMasterHostNodeCache(CuratorFramework)}
     * @return the master host/port string for the database or absent if no known master to use
     */
    @Nonnull
    Optional<String> getCurrentMasterForDatabase(String databaseId, PathChildrenCache zkHostNodeCache);

    /**
     * Creates the actual path child cache object that will be passed into each call to getCurrentMasterForDatabase,
     * allowing the implementor of this class to have complete control over how that is initialized and its path, etc.
     * @param zkClient the zookeeper client to use for the cache
     * @return the non-null cache object configured to watch the top-most directory for MySQL database master hosts'
     *          nodes
     */
    @Nonnull
    PathChildrenCache createMasterHostNodeCache(CuratorFramework zkClient);

    /**
     * Called whenever Zookeeper client becomes connected. This also provides a reference to the PathChildrenCache in
     * it must be rebuilt at this time. Generally, this is where an implementor would call something like
     * {@link PathChildrenCache#start(PathChildrenCache.StartMode)}
     */
    void onZookeeperConnected(@Nonnull CuratorFramework client, @Nonnull PathChildrenCache zkHostNodeCache,
                              boolean firstTimeConnected);
}
