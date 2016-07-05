package com.mysql.jdbc;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.mysql.jdbc.zk.ZookeeperDatabaseMasterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Zookeeper-driven implementation of the MultiHostConnectionProxy that works as an eager FailoverConnectionProxy that
 * uses a Zookeeper-provided master configuration instead of relying on a predefined set of hosts or a VIP. Returns
 * proxy instances of {@link ZookeeperDynamicMasterConnection}.
 *
 * @author dgarson
 *
 * @see FailoverConnectionProxy
 * @see LoadBalancedConnectionProxy
 */
public class ZookeeperDynamicMasterConnectionProxy extends MultiHostConnectionProxy {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperDynamicMasterConnectionProxy.class);

    private static final String METHOD_GET_DATABASE_IDENTIFIER = "getDatabaseIdentifier";
    private static final String METHOD_IS_CONNECTED_TO_CURRENT_MASTER = "isConnectedToCurrentMaster";
    private static final String METHOD_GET_LAST_THROWN_EXCEPTION = "getLastThrownException";
    private static final String METHOD_SET_READ_ONLY = "setReadOnly";
    private static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
    private static final String METHOD_COMMIT = "commit";
    private static final String METHOD_ROLLBACK = "rollback";

    private static Class<?>[] INTERFACES_TO_PROXY;

    static {
        if (Util.isJdbc4()) {
            try {
                INTERFACES_TO_PROXY = new Class<?>[] {
                        Class.forName("com.mysql.jdbc.JDBC4MySQLConnection"),
                        ZookeeperDynamicMasterConnection.class
                };
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            INTERFACES_TO_PROXY = new Class<?>[] {
                    MySQLConnection.class,
                    ZookeeperDynamicMasterConnection.class
            };
        }
    }

    private final String databaseId;
    private final ZookeeperDatabaseMasterRegistry masterRegistry;

    private Optional<String> currentMaster;

    /* state related to user operations on the Connection */
    private Boolean explicitlyReadOnly = null;
    private boolean explicitlyAutoCommit = true;

    /* state of the proxy */
    private boolean aborted = false;
    private boolean invalidated = false;

    public ZookeeperDynamicMasterConnectionProxy(Properties props, String databaseId) throws SQLException {
        super(Collections.<String>emptyList(), props);

        if (StringUtils.isNullOrEmpty(databaseId)) {
            throw new SQLException("Must provide valid databaseId, was blank or null");
        }

        this.databaseId = databaseId;
        this.masterRegistry = ZookeeperDatabaseMasterRegistry.getInstance();
        this.currentMaster = masterRegistry.getCurrentDatabaseMaster(databaseId);
        if (!currentMaster.isPresent()) {
            throw new SQLException("Unable to locate current master for database '" + databaseId + "' in Zookeeper");
        }

        pickNewConnection();

        this.explicitlyAutoCommit = this.currentConnection.getAutoCommit();
    }

    private Optional<String> getLatestMasterFromRegistry() {
        return masterRegistry.getCurrentDatabaseMaster(databaseId);
    }

    @Nonnull
    public String getDatabaseId() {
        return databaseId;
    }

    @Override
    InvocationHandler getNewJdbcInterfaceProxy(Object toProxy) {
        return new MasterSwitchoverJdbcInterfaceProxy(toProxy);
    }

    @Override
    final void dealWithInvocationException(InvocationTargetException e) throws Throwable {
        Throwable cause = e.getTargetException();
        if (cause != null) {
            if (this.lastExceptionDealtWith != cause && shouldExceptionTriggerConnectionSwitch(cause)) {
                invalidateAndEstablishConnection(null);
                this.lastExceptionDealtWith = cause;
            }
            throw cause;
        }
        throw e;
    }

    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        if (!(t instanceof SQLException)) {
            return false;
        }

        String sqlState = ((SQLException) t).getSQLState();
        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                // connection error
                return true;
            }
        }

        // Always handle CommunicationsException
        if (t instanceof CommunicationsException) {
            return true;
        }

        return false;
    }

    @Override
    boolean isMasterConnection() {
        // TODO(dgarson) investigate call throughput of this method
        return Objects.equal(this.currentMaster, getLatestMasterFromRegistry()) &&
                currentMaster != null && currentMaster.isPresent();
    }

    /**
     * Checks whether the master has been changed since the last check/usage of the connection and updates to point to
     * the new master, if it has changed.
     * @return true if the master had switched and was updated during this call, false otherwise
     */
    public boolean isConnectedToCurrentMaster() throws SQLException {
        Optional<String> latestMaster = getLatestMasterFromRegistry();
        if (Objects.equal(currentMaster, latestMaster)) {
            // if they've both been absent, throw an exception
            if (!latestMaster.isPresent()) {
                throw cannotFindMasterException();
            }
            return true;
        }
        // not connected to current master
        return false;
    }

    @Override
    synchronized void invalidateCurrentConnection() throws SQLException {
        super.invalidateCurrentConnection();
        invalidated = true;
    }

    private synchronized void invalidateAndEstablishConnection() throws SQLException {
        // passing in null will cause the latest to be retrieved from Zookeeper (live) cache
        invalidateAndEstablishConnection(/*master=*/null);
    }

    protected synchronized void invalidateAndEstablishConnection(Optional<String> master) throws SQLException {
        // use master provided if present
        if (master == null || !master.isPresent()) {
            master = masterRegistry.getCurrentDatabaseMaster(databaseId);
        }

        // invalidate connection now
        if (!invalidated) {
            invalidateCurrentConnection();
            log.debug("Invalidated connection for {}", databaseId);
        }

        // if we have no master to connect to, fail fast
        if (!master.isPresent()) {
            throw cannotFindMasterException();
        }

        // establish a physical connection to the current master
        MySQLConnection connection = createConnectionForHost(master.get());
        log.debug("Established new connection to database '{}' at: {}", databaseId, master.get());

        // set proper read-only state
        boolean readOnly;
        if (this.explicitlyReadOnly != null) {
            readOnly = this.explicitlyReadOnly;
        } else if (this.currentConnection != null) {
            readOnly = this.currentConnection.isReadOnly();
        } else {
            readOnly = false;
        }

        syncSessionState(this.currentConnection, connection, readOnly);
        this.currentMaster = master;
        this.currentConnection = connection;
        this.aborted = false;
        this.invalidated = false;
        this.lastExceptionDealtWith = null;
    }

    @Override
    synchronized void pickNewConnection() throws SQLException {
        pickNewConnection(/*force=*/false);
    }

    synchronized void pickNewConnection(boolean force) throws SQLException {
        if (this.isClosed && this.closedExplicitly) {
            return;
        }

        if (force || !isConnected()) {
            invalidateAndEstablishConnection();
        }
    }

    /**
     * Closes current connection.
     */
    @Override
    synchronized void doClose() throws SQLException {
        this.currentConnection.close();
    }

    /**
     * Aborts current connection.
     */
    @Override
    synchronized void doAbortInternal() throws SQLException {
        this.currentConnection.abortInternal();
        aborted = true;
    }

    /**
     * Aborts current connection using the given executor.
     */
    @Override
    synchronized void doAbort(Executor executor) throws SQLException {
        this.currentConnection.abort(executor);
        aborted = true;
    }

    boolean isConnected() {
        return !invalidated && !aborted && !isClosed &&
                currentMaster != null && currentMaster.isPresent() &&
                currentConnection != null;
    }

    private SQLException cannotFindMasterException() {
        return new SQLException("Unable to locate master Zookeeper node for database '" + databaseId + "'");
    }

    /*
     * Local method invocation handling for this proxy.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    public synchronized Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (METHOD_GET_DATABASE_IDENTIFIER.equals(methodName)) {
            return databaseId;
        } else if (METHOD_IS_CONNECTED_TO_CURRENT_MASTER.equals(methodName)) {
            return isConnectedToCurrentMaster();
        } else if (METHOD_GET_LAST_THROWN_EXCEPTION.equals(methodName)) {
            return lastExceptionDealtWith;
        }

        if (METHOD_SET_READ_ONLY.equals(methodName)) {
            this.explicitlyReadOnly = (Boolean) args[0];
        }

        if (this.isClosed && !allowedOnClosedConnection(method)) {
            if (this.autoReconnect && !this.closedExplicitly) {
                // Act as if this is the first connection but let it sync with the previous one.
                pickNewConnection(/*force=*/true);
                this.isClosed = false;
                this.closedReason = null;
            } else {
                String reason = "No operations allowed after connection closed.";
                if (this.closedReason != null) {
                    reason += ("  " + this.closedReason);
                }
                throw SQLError.createSQLException(reason, SQLError.SQL_STATE_CONNECTION_NOT_OPEN, null /* no access to a interceptor here... */);
            }
        }

        Object result = null;

        try {
            result = method.invoke(this.thisAsConnection, args);
            result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
        } catch (InvocationTargetException e) {
            dealWithInvocationException(e);
        }

        if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
            this.explicitlyAutoCommit = (Boolean) args[0];
        }

        if ((this.explicitlyAutoCommit || METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName))
                && !isMasterConnection()) {
            // switch over to the new host at transaction boundary
            invalidateAndEstablishConnection(null);
        }

        return result;
    }

    /**
     * Creates a new Connection that uses a Zookeeper-backed MySQL hostname and port for its master and performs an
     * automatic switch over to any new masters when Zookeeper indicates such a change has been made.
     * @param props the connection properties
     * @param databaseId the database identifier, which must have a configuration in Zookeeper
     * @return the Zookeeper master-master-switchover-supporting Connection proxy
     * @throws SQLException if there are any exceptions connecting to the database or locating the master in Zookeeper
     */
    public static Connection createProxyInstance(Properties props, String databaseId) throws SQLException {
        ZookeeperDynamicMasterConnectionProxy connProxy = new ZookeeperDynamicMasterConnectionProxy(props, databaseId);
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), INTERFACES_TO_PROXY, connProxy);
    }

    /**
     * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
     * Additionally intercepts query executions and triggers an execution count on the outer class.
     */
    class MasterSwitchoverJdbcInterfaceProxy extends JdbcInterfaceProxy {
        MasterSwitchoverJdbcInterfaceProxy(Object toInvokeOn) {
            super(toInvokeOn);
        }

        @SuppressWarnings("synthetic-access")
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            boolean isExecute = methodName.startsWith("execute");
            Object result = super.invoke(proxy, method, args);

            if (ZookeeperDynamicMasterConnectionProxy.this.explicitlyAutoCommit && isExecute &&
                    !isMasterConnection()) {
                // Switch to new master host at transaction boundary
                invalidateAndEstablishConnection(null);
            }

            return result;
        }
    }

}
