package com.mysql.jdbc.c3p0;

import com.mchange.v2.c3p0.AbstractConnectionTester;
import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mysql.jdbc.CommunicationsException;
import com.mysql.jdbc.ZookeeperDynamicMasterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * c3p0-specific ConnectionTester implementation that takes advantage of both our dynamic master reconfiguration and the
 * MySQL "PING" command that bypasses the need to do a SELECT query when testing liveliness of connections. <br/>
 * Largely taken from {@link com.mysql.jdbc.integration.c3p0.MysqlConnectionTester}
 *
 * @author dgarson
 */
public class ZookeeperDynamicMasterConnectionTester extends AbstractConnectionTester {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperDynamicMasterConnectionTester.class);

    private static final String COMMUNICATION_EXCEPTION_CLASS_NAME =
            "com.mysql.jdbc.exceptions.jdbc4.CommunicationsException";

    private static final String DEFAULT_TEST_QUERY = "SELECT 1";
    private static final Object[] NO_ARGS_ARRAY = new Object[0];

    private Method pingMethod;
    private Method isMasterConnectionMethod;
    private Method isConnectedToCurrentMasterMethod;
    private volatile Exception lastException;

    public ZookeeperDynamicMasterConnectionTester() {
        try {
            this.pingMethod = com.mysql.jdbc.Connection.class.getMethod("ping", (Class[]) null);
        } catch (Exception ex) {
            log.warn("Unable to locate com.mysql.jdbc.Connection#ping() method, falling back to SELECT 1 method!", ex);
            // punt, we have no way to recover, other than we now use 'SELECT 1' for handling the connection testing.
        }
        try {
            this.isMasterConnectionMethod = com.mysql.jdbc.MySQLConnection.class.getMethod("isMasterConnection",
                    (Class[])null);
        } catch (Exception ex) {
            log.warn("Unable to locate MySQLConnection#isMasterConnection() method!", ex);
            // swallowed
        }
        try {
            this.isConnectedToCurrentMasterMethod = ZookeeperDynamicMasterConnection.class.getMethod(
                    "isConnectedToCurrentMaster", (Class[])null);
        } catch (Exception ex) {
            log.error("Unable to locate ZookeeperDynamicMasterConnection#isConnectedToCurrentMaster() method!", ex);
            // swallowed
        }
    }

    public Exception getLastException() {
        return lastException;
    }

    private int activeCheckConnectionWithQuery(Connection connection, String preferredTestQuery,
                                               Throwable[] rootCauseOutParamHolder) {
        Statement pingStatement = null;
        try {
            pingStatement = connection.createStatement();
            pingStatement.executeQuery(preferredTestQuery != null ? preferredTestQuery : DEFAULT_TEST_QUERY).close();
            return CONNECTION_IS_OKAY;
        } catch (Exception e) {
            rootCauseOutParamHolder[0] = e;
            lastException = e;
            return CONNECTION_IS_INVALID;
        } finally {
            try {
                if (pingStatement != null) {
                    pingStatement.close();
                }
            } catch (Exception e) {
                // swallowed
            }
        }
    }

    @Override
    public int activeCheckConnection(Connection conn, String preferredTestQuery, Throwable[] rootCauseOutParamHolder) {
        try {
            // try to check if not connected to current master -- if we're using c3p0 and somehow we're not using this
            //      driver then this may fail since it won't be an instance of ZookeeperDynamicMasterConnection itfc.
            if (isConnectedToCurrentMasterMethod != null) {
                Boolean isConnectedToMaster;
                try {
                    if (conn instanceof C3P0ProxyConnection) {
                        isConnectedToMaster = (boolean) ((C3P0ProxyConnection)conn)
                                .rawConnectionOperation(isConnectedToCurrentMasterMethod, conn, NO_ARGS_ARRAY);
                    } else if (conn instanceof ZookeeperDynamicMasterConnection) {
                        isConnectedToMaster = ((ZookeeperDynamicMasterConnection)conn).isConnectedToCurrentMaster();
                    } else {
                        log.warn("Connection type '{}' is not supported for isConnectedToCurrentMaster checks",
                                conn.getClass());
                        isConnectedToMaster = null;
                    }
                } catch (Exception e) {
                    // swallow and fall through
                    isConnectedToMaster = null;
                }
                if (isConnectedToMaster != null && !isConnectedToMaster) {
                    return CONNECTION_IS_INVALID;
                }
            }

            if (isMasterConnectionMethod != null) {
                Boolean isMasterConnected = null;
                if (conn instanceof com.mysql.jdbc.Connection) {
                    isMasterConnected = ((com.mysql.jdbc.Connection)conn).isMasterConnection();
                } else if (conn instanceof C3P0ProxyConnection) {
                    // assume wrapped connection is a MySQLConnection
                    isMasterConnected = (boolean) ((C3P0ProxyConnection)conn)
                            .rawConnectionOperation(isMasterConnectionMethod, conn, NO_ARGS_ARRAY);
                }
                // return as invalid if we are currently not connected to master, allowing c3p0 to preemptively reap
                //  before we switch over
                if (isMasterConnected != null && !isMasterConnected) {
                    return CONNECTION_IS_INVALID;
                }
            }

            if (pingMethod != null) {
                if (conn instanceof com.mysql.jdbc.Connection) {
                    // We've been passed an instance of a MySQL connection -- no need for reflection
                    ((com.mysql.jdbc.Connection) conn).ping();
                } else {
                    // Assume the connection is a C3P0 proxy
                    C3P0ProxyConnection castCon = (C3P0ProxyConnection) conn;
                    castCon.rawConnectionOperation(this.pingMethod, C3P0ProxyConnection.RAW_CONNECTION, NO_ARGS_ARRAY);
                }
                // if we get this far, we successfully pinged the server
                return CONNECTION_IS_OKAY;
            }

            // if we are unable to use the ping method and the isMasterConnected returned true, then we must
            return activeCheckConnectionWithQuery(conn, preferredTestQuery, rootCauseOutParamHolder);
        } catch (Exception e) {
            rootCauseOutParamHolder[0] = e;
            lastException = e;
            return CONNECTION_IS_INVALID;
        }
    }

    @Override
    public int statusOnException(Connection c, Throwable throwable, String preferredTestQuery,
                                 Throwable[] rootCauseOutParamHolder) {
        if (throwable instanceof CommunicationsException ||
                COMMUNICATION_EXCEPTION_CLASS_NAME.equals(throwable.getClass().getName())) {
            return CONNECTION_IS_INVALID;
        }

        if (throwable instanceof SQLException) {
            String sqlState = ((SQLException) throwable).getSQLState();

            if (sqlState != null && sqlState.startsWith("08")) {
                return CONNECTION_IS_INVALID;
            }

            return CONNECTION_IS_OKAY;
        }

        // Runtime/Unchecked?
        return CONNECTION_IS_INVALID;
    }
}
