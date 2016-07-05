package com.mysql.jdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;

/**
 * @author dgarson
 */
public interface ZookeeperDynamicMasterConnection extends MySQLConnection {

    /**
     * Returns the database identifier for this connection.
     */
    @Nonnull String getDatabaseIdentifier();

    /**
     * Checks whether the master this connection is using is the same as the master currently published by Zookeeper for
     * the database associated with this connection.
     * @return true if using latest master, false otherwise
     */
    boolean isConnectedToCurrentMaster() throws SQLException;

    /**
     * Returns the last throwable exception caught related to this connection. This is reset whenever a new connection
     * is successfully established to the database.
     */
    @Nullable Throwable getLastThrownException();
}
