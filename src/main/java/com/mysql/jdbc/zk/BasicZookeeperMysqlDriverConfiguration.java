package com.mysql.jdbc.zk;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import javax.annotation.Nonnull;

/**
 * Created by dgarson on 7/4/16.
 */
public class BasicZookeeperMysqlDriverConfiguration implements ZookeeperMysqlDriverConfiguration {

    public static final String PROPERTY_ZOOKEEPER_ADDRESSES = "mysql.zk.addresses";
    public static final String PROPERTY_DATACENTER = "mysql.zk.datacenter";
    /**
     * Format string expected to have 3 instances of '%s' for formatting: the datacenter, the environment, and
     * the database ID
     */
    public static final String PROPERTY_ZOOKEEPER_PATH_FORMAT = "mysql.zk.path_format";

    // DC/Env/dbName
    private static final String DEFAULT_ZOOKEEPER_PATH_FORMAT = "/%s/%s/%s";

    /**
     * The environment name to use in the Zookeeper paths.
     */
    public static final String PROPERTY_ENVIRONMENT = "mysql.zk.environment";
    /**
     * Optional property for maximum cache time for [master] hostnames in milliseconds.
     */
    public static final String PROPERTY_CACHE_DURATION_MILLIS = "mysql.zk.host_cache_duration";

    private final String hostAddresses;
    private final String datacenterName;
    private final String environmentName;
    private final Optional<Long> hostnameCacheDurationMillis;

    private final String pathFormat;
    private final String watchPath;

    private final LoadingCache<String, String> databaseToNodePathCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, String>() {
                @Override
                public String load(String databaseId) throws Exception {
                    return String.format(pathFormat, databaseId, environmentName, databaseId);
                }
            });

    public BasicZookeeperMysqlDriverConfiguration() {
        hostAddresses = getProperty(PROPERTY_ZOOKEEPER_ADDRESSES, true);
        String strCacheDuration = getProperty(PROPERTY_CACHE_DURATION_MILLIS, false);
        if (strCacheDuration != null && strCacheDuration.length() > 0) {
            try {
                hostnameCacheDurationMillis = Optional.of(Long.parseLong(strCacheDuration));
            } catch (NumberFormatException nfe) {
                throw new IllegalStateException("Invalid, non-number value for '" + PROPERTY_CACHE_DURATION_MILLIS +
                        "': " + strCacheDuration);
            }
        } else {
            hostnameCacheDurationMillis = Optional.absent();
        }

        String pathFmt = getProperty(PROPERTY_ZOOKEEPER_PATH_FORMAT, false);
        if (pathFmt == null) {
            pathFmt = DEFAULT_ZOOKEEPER_PATH_FORMAT;
        }
        pathFormat = pathFmt.startsWith("/") ? pathFmt : "/" + pathFmt;
        datacenterName = getProperty(PROPERTY_DATACENTER, true);
        environmentName = getProperty(PROPERTY_ENVIRONMENT, true);

        // we should end up with "[/]datacenter/environment/%s"
        String basePath = String.format(pathFormat, datacenterName, environmentName);
        int lastSlash = basePath.lastIndexOf('/');
        this.watchPath = basePath.substring(0, lastSlash);
    }

    @Nonnull
    @Override
    public String getHostAddresses() {
        return hostAddresses;
    }

    @Override
    public void configureClient(@Nonnull CuratorFrameworkFactory.Builder clientBuilder) {
        // no-op?
    }

    @Nonnull
    @Override
    public Optional<Long> getHostnameCacheDurationMillis() {
        return hostnameCacheDurationMillis;
    }

    @Nonnull
    @Override
    public Optional<String> getCurrentMasterForDatabase(String databaseId, PathChildrenCache zkNodeTree) {
        String zkPath = databaseToNodePathCache.getUnchecked(databaseId);
        ChildData data = zkNodeTree.getCurrentData(zkPath);
        return data == null ? Optional.<String>absent() : Optional.of(String.valueOf(data.getData()));
    }

    @Nonnull
    @Override
    public PathChildrenCache createMasterHostNodeCache(CuratorFramework zkClient) {
        // create cache of endpoints for each databaseId
        PathChildrenCache cache = new PathChildrenCache(zkClient, watchPath, true,
                new ThreadFactoryBuilder().setNameFormat("mysql-master-watcher-%d").build());
        return cache;
    }

    @Override
    public void onZookeeperConnected(@Nonnull CuratorFramework client, @Nonnull PathChildrenCache zkHostNodeCache,
                                     boolean firstTimeConnected) {
        try {
            if (firstTimeConnected) {
                // block for full cache priming on startup
                zkHostNodeCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            } else {
                // clear and allow cache to repopulate when reconnecting to ZK after startup
                zkHostNodeCache.clearAndRefresh();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to build ZK node cache path for watch directory '" + watchPath + "'", e);
        }
    }

    private static String getProperty(String propName, boolean required) {
        String value = System.getProperty(propName);
        if (required && (value == null || value.length() == 0)) {
            throw new IllegalStateException("Missing required system property: '" + propName + "'");
        }
        return value;
    }

}
