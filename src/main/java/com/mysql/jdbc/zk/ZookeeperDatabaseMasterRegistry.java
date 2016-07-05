package com.mysql.jdbc.zk;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author dgarson
 */
public class ZookeeperDatabaseMasterRegistry {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperDatabaseMasterRegistry.class);

    private static final ZookeeperDatabaseMasterRegistry instance = createSingleton();

    private static final String SYSPROP_ZOOKEEPER_DRIVER_CONFIGURATION_CLASS =
            "mysql.zookeeper.configuration_class";

    public static final int CONNECTION_TIMEOUT_MILLIS_DEFAULT = 250;
    public static final int SESSION_TIMEOUT_MILLIS_DEFAULT = 40000;

    private ZookeeperMysqlDriverConfiguration driverConfig;

    private CuratorFramework client;
    private ConnectionStateListener connectionStateListener;

    private final String masterWatchPath;
    private final PathChildrenCache masterEndpointsCache;
    private final AtomicBoolean everConnected = new AtomicBoolean(false);

    @Nullable
    private final LoadingCache<String, Optional<String>> databaseToCurrentHostnameCache;

    private ZookeeperDatabaseMasterRegistry(@Nonnull final ZookeeperMysqlDriverConfiguration driverConfig) {
        Preconditions.checkArgument(driverConfig != null, "driverConfig must be non-null for this constructor!");
        this.driverConfig = driverConfig;
        String addresses = driverConfig.getHostAddresses();
        if (StringUtils.isBlank(addresses)) {
            throw new IllegalStateException("Unable to find any Zookeeper addresses returned from driver " +
                    "configuration of type " + driverConfig.getClass());
        }
        String nodePath = driverConfig.getNodeWatchDirectory();
        // trim trailing forward slash
        if (nodePath.endsWith("/")) {
            nodePath = nodePath.substring(0, nodePath.length() - 1);
        }
        this.masterWatchPath = nodePath;

        // start zookeeper client and initialize master host/port configurations
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 2);
        //default sessionTimeout is 60s and default connectionTimeout is 15s.
        client = CuratorFrameworkFactory.newClient(addresses, SESSION_TIMEOUT_MILLIS_DEFAULT,
                CONNECTION_TIMEOUT_MILLIS_DEFAULT, retryPolicy);
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                if (!connectionState.isConnected()) {
                    log.warn("Disconnected from Zookeeper cluster for MySQL DB master registry");
                } else {
                    onConnected();
                }
                if(connectionStateListener != null) {
                    connectionStateListener.stateChanged(curatorFramework, connectionState);
                }
            }
        });
        client.start();

        // create cache of endpoints for each databaseId
        masterEndpointsCache = new PathChildrenCache(client, masterWatchPath, true,
                new ThreadFactoryBuilder().setNameFormat("mysql-master-watcher-%d").build());

        Optional<Long> cacheDurationMillis = driverConfig.getHostnameCacheDurationMillis();
        if (cacheDurationMillis != null && cacheDurationMillis.isPresent() && cacheDurationMillis.get() > 0) {
            databaseToCurrentHostnameCache = CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheDurationMillis.get(), TimeUnit.MILLISECONDS)
                    .build(new CacheLoader<String, Optional<String>>() {
                        @Override
                        public Optional<String> load(String databaseId) throws Exception {
                            return driverConfig.getCurrentMasterForDatabase(databaseId, masterEndpointsCache);
                        }
                    });
        } else {
            databaseToCurrentHostnameCache = null;
        }
    }

    public ZookeeperDatabaseMasterRegistry() {
        this(createDriverConfiguration());
    }

    /**
     * Returns the current database host and port that should be used for a given database.
     * @param databaseId the database ID
     * @return a non-null host/port to use for the current master
     * @throws IllegalStateException if no host/port could be resolved for <tt>databaseId</tt> in Zookeeper
     */
    public String getCurrentDatabaseMasterOrException(String databaseId) {
        Optional<String> master = getCurrentDatabaseMaster(databaseId);
        if (!master.isPresent()) {
            throw new IllegalStateException("Unable to resolve any known host/port for database with id '" +
                    databaseId + "'.");
        }
        return master.get();
    }

    /**
     * Returns the current database host and port that should be used for a given database.
     * @param databaseId the database ID
     * @return the current database master or absent if not found in Zookeeper
     */
    public Optional<String> getCurrentDatabaseMaster(String databaseId) {
        Optional<String> cachedHost = databaseToCurrentHostnameCache != null ?
                databaseToCurrentHostnameCache.getUnchecked(databaseId) : null;
        if (cachedHost != null && cachedHost.isPresent()) {
            return cachedHost;
        }
        return driverConfig.getCurrentMasterForDatabase(databaseId, masterEndpointsCache);
    }

    private void onConnected() {
        try {
            log.info("Connected to Zookeeper cluster for MySQL DB master registry");

            // eagerly populate the cache of endpoints
            if (everConnected.compareAndSet(false, true)) {
                masterEndpointsCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            } else {
                masterEndpointsCache.rebuild();
            }
        } catch (Exception e) {
            log.error("Unable to load/refresh the MySQL master registry from Zookeeper", e);
        }
    }

    public void setConnectionStateListener(ConnectionStateListener connectionStateListener) {
        this.connectionStateListener = connectionStateListener;
    }

    public void shutdown(){
        if (client != null){
            client.close();
        }
    }

    public static ZookeeperDatabaseMasterRegistry getInstance() {
        return instance;
    }

    private static ZookeeperDatabaseMasterRegistry createSingleton() {
        ZookeeperDatabaseMasterRegistry registry;
        try {
            registry = new ZookeeperDatabaseMasterRegistry(null);
        } catch (Exception e) {
            String msg = "Unable to initialize database master registry for MySQL using Zookeeper";
            log.error(msg, e);
            throw (Error)new ExceptionInInitializerError(msg).initCause(e);
        }
        return registry;
    }

    private static ZookeeperMysqlDriverConfiguration createDriverConfiguration() {
        String driverConfigClassName = System.getProperty(SYSPROP_ZOOKEEPER_DRIVER_CONFIGURATION_CLASS);
        if (driverConfigClassName == null) {
            throw new IllegalStateException("Unable to use MySQL Zookeeper driver without driver configuration " +
                    "class provided in system property: '" + SYSPROP_ZOOKEEPER_DRIVER_CONFIGURATION_CLASS + "'");
        }
        Class<? extends ZookeeperMysqlDriverConfiguration> driverConfigClass;
        try {
            driverConfigClass = Class.forName(driverConfigClassName)
                    .asSubclass(ZookeeperMysqlDriverConfiguration.class);
        } catch (ClassCastException | ClassNotFoundException e) {
            throw new IllegalStateException("Unable to resolve/cast driver config class: " + driverConfigClassName, e);
        }
        try {
            return driverConfigClass.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to instantiate driver configuration class of type " +
                driverConfigClassName, e);
        }
    }
}
