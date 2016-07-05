package com.mysql.jdbc;

import com.google.common.base.Optional;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Zookeeper-driven MySQL dynamic master driver wrapper. Uses a Zookeeper MultiHostConnectionProxy to automatically
 * switch connections to an "old" master to the new one. The current master will always be determined based on a
 * Zookeeper node path containing the database name included in the JDBC URL.
 *
 * @author dgarson
 */
public class ZookeeperDynamicMasterMysqlDriver implements Driver {

    private static final String JDBC_PREFIX = "jdbc:zkmysql//";

    static {
        try {
            java.sql.DriverManager.registerDriver(new ZookeeperDynamicMasterMysqlDriver());
        } catch (SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    private final com.mysql.jdbc.Driver driver;
    private final Logger logger = Logger.getLogger(getClass().getName());

    public ZookeeperDynamicMasterMysqlDriver() throws SQLException {
        // attempt to re-use existing Driver instance so we don't have to create a second instance (which is
        // technically fine)
        Optional<com.mysql.jdbc.Driver> registeredDriver = findDriver(DriverManager.getDrivers(), com.mysql.jdbc.Driver.class);
        driver = registeredDriver.isPresent() ? registeredDriver.get() : new com.mysql.jdbc.Driver();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && StringUtils.startsWithIgnoreCase(url, JDBC_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String configStr = url.substring(JDBC_PREFIX.length());
        int qsIdx = configStr.indexOf('?');
        String databaseId = configStr.substring(0, qsIdx < 0 ? configStr.length() : qsIdx);
        return ZookeeperDynamicMasterConnectionProxy.createProxyInstance(info, databaseId);
    }

    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return logger;
    }

    private static <D extends Driver> Optional<D> findDriver(Enumeration<Driver> drivers, Class<D> driverType) {
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driverType.isInstance(driver)) {
                return Optional.of(driverType.cast(driver));
            }
        }
        return Optional.absent();
    }
}
