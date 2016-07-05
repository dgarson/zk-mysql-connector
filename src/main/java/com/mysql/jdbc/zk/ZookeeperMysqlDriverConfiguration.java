package com.mysql.jdbc.zk;

import com.google.common.base.Optional;
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
     * @param zkNodeTree the Zookeeper-backed path-children node data cache based on a watch on the path returned from
     *                      {@link #getNodeWatchDirectory()}
     * @return the master host/port string for the database or absent if no known master to use
     */
    @Nonnull
    Optional<String> getCurrentMasterForDatabase(String databaseId, PathChildrenCache zkNodeTree);

    /**
     * Returns the top-most parent directory within which all Zookeeper nodes used for master configuration with this
     * MySQL driver live. Therefore all values of {@link #getCurrentMasterForDatabase(String, PathChildrenCache)} for
     * all database IDs must be under the returned path. This path may be environment-specific.
     */
    @Nonnull
    String getNodeWatchDirectory();
}
