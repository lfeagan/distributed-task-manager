package net.vectorcomputing.dtm.postgresql;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static net.vectorcomputing.dtm.postgresql.ToxiproxyUtils.reflectivelyGetToxiproxyClient;

public class TimescaleTestContainer {

    protected static final DockerImageName TIMESCALEDB_IMAGE = DockerImageName.parse("timescale/timescaledb-ha").asCompatibleSubstituteFor("postgres");
    public static final String TIMESCALE_DEFAULT_TAG = "pg14-latest";
    protected static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");

    // An alias that can be used to resolve the Toxiproxy container by name in the network it is connected to.
    // It can be used as a hostname of the Toxiproxy container by other containers in the same network.
    protected static final String TOXIPROXY_NETWORK_ALIAS = "timescale-toxiproxy";

    private final Network network;
    private final String timescaleImageTag;
    private JdbcDatabaseContainer timescaleContainer = null;
    private String timescaleJdbcUrl = null;

    private String dbInitScriptPath = "timescale_init.sql";

    // Toxiproxy container, which will be used as a TCP proxy
    private final boolean toxiproxyEnabled;
    private ToxiproxyContainer toxiproxyContainer = null;
    private ToxiproxyContainer.ContainerProxy proxy = null;
    private String toxiproxyJdbcUrl = null;

    public TimescaleTestContainer() {
        this.network = Network.newNetwork();
        this.toxiproxyEnabled = true;
        this.timescaleImageTag = TIMESCALE_DEFAULT_TAG;
    }

    public TimescaleTestContainer(Network network) {
        this(network, TIMESCALE_DEFAULT_TAG, true);
    }

    public TimescaleTestContainer(Network network, String timescaleImageTag, boolean useToxiproxy) {
        this.network = network;
        this.timescaleImageTag = timescaleImageTag;
        this.toxiproxyEnabled = useToxiproxy;
    }

    public String getTimescaleImageTag() {
        return timescaleImageTag;
    }

    public boolean isToxiproxyEnabled() {
        return toxiproxyEnabled;
    }

    public synchronized void clearDbInitScriptPath() {
        this.dbInitScriptPath = null;
    }

    public synchronized void setDbInitScriptPath(final String dbInitScriptPath) {
        this.dbInitScriptPath = dbInitScriptPath;
    }

    public synchronized String getDbInitScriptPath() {
        return this.dbInitScriptPath;
    }

    public synchronized void start() {
        if (timescaleContainer != null) {
            return;
        }

        timescaleContainer = (JdbcDatabaseContainer) new PostgreSQLContainer(TIMESCALEDB_IMAGE.withTag(timescaleImageTag))
                .withNetworkAliases("timescaledb");
        if (dbInitScriptPath != null) {
            timescaleContainer.withInitScript(dbInitScriptPath);
        }
        timescaleContainer.withNetwork(network);

        timescaleContainer.start();
        timescaleJdbcUrl = timescaleContainer.getJdbcUrl();

        if (toxiproxyEnabled) {
            toxiproxyContainer = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                   .withNetwork(network)
                    .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
            toxiproxyContainer.start();
            int jdbcPort = (Integer) timescaleContainer.getExposedPorts().get(0);
            proxy = toxiproxyContainer.getProxy(timescaleContainer, jdbcPort);
            toxiproxyJdbcUrl = "jdbc:postgresql://" + proxy.getContainerIpAddress() + ":" + proxy.getProxyPort() + "/test?loggerLevel=OFF&loginTimeout=2";
        } else {
            toxiproxyJdbcUrl = null;
        }

        assert(timescaleContainer != null); // "Database container instance is not null as expected"
        assert(timescaleContainer.isRunning()); //"Database container is running as expected"
    }

    public synchronized void stop() {
        if (timescaleContainer != null) {
            timescaleContainer.stop();
            timescaleContainer = null;
        }
        if (toxiproxyContainer != null){
            toxiproxyContainer.stop();
            toxiproxyContainer = null;
        }
    }

    public synchronized Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), "test", "test");
    }

    public synchronized Connection getConnection(Properties properties) throws SQLException {
        return DriverManager.getConnection(getJdbcUrl(), properties);
    }

    public synchronized String getUser() {
        return "test";
    }

    public String getPassword() {
        return "test";
    }

    public String getHostname() {
        if (toxiproxyEnabled) {
            return proxy.getContainerIpAddress();
        } else {
            return (String) timescaleContainer.getNetworkAliases().get(0);
        }
    }

    public int getPort() {
        if (toxiproxyEnabled) {
            return proxy.getProxyPort();
        } else {
            return (Integer) timescaleContainer.getExposedPorts().get(0);
        }
    }

    public synchronized String getInternalJdbcUrl() {
        return "jdbc:postgresql://timescaledb:5432/test";
    }

    public synchronized String getJdbcUrl() {
        if (timescaleContainer == null) {
            throw new RuntimeException("Timescale container has not been started");
        }
        if (toxiproxyEnabled) {
            return toxiproxyJdbcUrl;
        } else {
            return timescaleJdbcUrl;
        }
    }

    public synchronized String getTimescaleJdbcUrl() {
        if (timescaleContainer == null) {
            throw new RuntimeException("Timescale container has not been started");
        }
        return timescaleJdbcUrl;
    }

    public synchronized JdbcDatabaseContainer getContainer() {
        if (timescaleContainer == null) {
            throw new RuntimeException("Timescale container has not been started");
        }
        return timescaleContainer;
    }

    public synchronized String getToxiproxyJdbcUrl() {
        if (toxiproxyContainer == null) {
            throw new RuntimeException("Toxiproxy container has not been started");
        }
        return toxiproxyJdbcUrl;
    }

    public synchronized ToxiproxyContainer.ContainerProxy getProxy() {
        if (proxy == null) {
            throw new RuntimeException("Timescale container has not been started");
        }
        return proxy;
    }

    public String getToxiproxyKey() {
        return timescaleContainer.getNetworkAliases().get(0) + ":" + timescaleContainer.getExposedPorts().get(0);
    }

    public void disableToxiproxy() {
        try {
            ToxiproxyClient toxiproxyClient = reflectivelyGetToxiproxyClient(toxiproxyContainer);
            Proxy proxy = toxiproxyClient.getProxy(getToxiproxyKey());
            proxy.disable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void enableToxiproxy() {
        try {
            ToxiproxyClient toxiproxyClient = reflectivelyGetToxiproxyClient(toxiproxyContainer);
            Proxy proxy = toxiproxyClient.getProxy(getToxiproxyKey());
            proxy.enable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteToxiproxy() {
        try {
            ToxiproxyClient toxiproxyClient = reflectivelyGetToxiproxyClient(toxiproxyContainer);
            Proxy proxy = toxiproxyClient.getProxy(getToxiproxyKey());
            proxy.delete();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
