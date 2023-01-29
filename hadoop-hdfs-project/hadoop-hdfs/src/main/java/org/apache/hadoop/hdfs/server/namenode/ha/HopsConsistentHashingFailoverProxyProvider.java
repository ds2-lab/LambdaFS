package org.apache.hadoop.hdfs.server.namenode.ha;

import com.google.common.hash.Hashing;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.retry.FailoverProxyProvider;

import com.google.common.base.Preconditions;
import io.hops.leader_election.node.ActiveNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.ha.FailoverProxyHelper.AddressRpcProxyPair;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import static com.google.common.hash.Hashing.consistentHash;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.google.common.hash.Hashing.consistentHash;

/**
 * Consistently hash the namespace over the available name node proxies.
 */
public class HopsConsistentHashingFailoverProxyProvider<T> implements
        FailoverProxyProvider<T> {

    public static final Log LOG =
            LogFactory.getLog(HopsConsistentHashingFailoverProxyProvider.class);

    private final Configuration conf;
    private final List<AddressRpcProxyPair<T>> proxies =
            new ArrayList<AddressRpcProxyPair<T>>();
    private final Map<Integer, List<AddressRpcProxyPair<T>>> proxiesByDomainId =
            new HashMap();

    private final UserGroupInformation ugi;
    private final Class<T> xface;
    private final Random rand = new Random((UUID.randomUUID()).hashCode());
    private final URI uri;

    protected String name = this.getClass().getSimpleName()+" ("+this.hashCode()+") ";

    private final int locationDomainId;

    public HopsConsistentHashingFailoverProxyProvider(Configuration conf, URI uri,
                                                      Class<T> xface) {
        Preconditions.checkArgument(
                xface.isAssignableFrom(NamenodeProtocols.class),
                "Interface class %s is not a valid NameNode protocol!");
        this.xface = xface;

        this.conf = new Configuration(conf);
        int maxRetries = this.conf.getInt(
                DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY,
                DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT);
        this.conf.setInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
                maxRetries);

        int maxRetriesOnSocketTimeouts = this.conf.getInt(
                DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
        this.conf.setInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                maxRetriesOnSocketTimeouts);

        this.locationDomainId = conf.getInt(DFSConfigKeys.DFS_LOCATION_DOMAIN_ID,
                DFSConfigKeys.DFS_LOCATION_DOMAIN_ID_DEFAULT);

        try {
            ugi = UserGroupInformation.getCurrentUser();

            this.uri = uri;

            List<ActiveNode> anl = FailoverProxyHelper.getActiveNamenodes(conf, xface, ugi, uri);
            updateProxies(anl);

            // The client may have a delegation token set for the logical
            // URI of the cluster. Clone this token to apply to each of the
            // underlying IPC addresses so that the IPC code can find it.
            //  HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ProxyInfo<T> getProxy(String target) {
        int idx = consistentHash(Hashing.md5().hashString(DFSUtil.extractParentPath(target)), proxies.size());

        return getProxyInternal(idx);
    }

    @Override
    public ProxyInfo<T> getProxy() {
        int randomIdx = rand.nextInt(proxies.size());

        return getProxyInternal(randomIdx);
    }

    /**
     * Get (and/or possibly create) the proxy associated with the RPC address at the given index.
     *
     * The index refers to the index in the list of `AddressRpcProxyPair` instances.
     *
     * @param idx The index into the list of `AddressRpcProxyPair` instances for which we will return a proxy, creating
     *            a new proxy if one does not already exist for the particular `AddressRpcProxyPair` instance.
     */
    private ProxyInfo<T> getProxyInternal(int idx) {
        AddressRpcProxyPair<T> current = proxies.get(idx);
        if (current.namenode == null) {
            try {
                current.namenode = NameNodeProxies.createNonHAProxy(conf,
                        current.address, xface, ugi, false).getProxy();
            } catch (IOException e) {
                LOG.error(name + " failed to create RPC proxy to NameNode", e);
                throw new RuntimeException(e);
            }
        }

        LOG.debug(name + " returning proxy for index: " + idx + " address: " +
                "" + current .address + " " + "Total proxies are: " + proxies.size());
        return new ProxyInfo<>(current.namenode, null);
    }

    @Override
    public synchronized void performFailover(T currentProxy) {
        throw new NotImplementedException("Fail-over is not supported when consistently hashing the namespace.");
    }

    @Override
    public Class<T> getInterface() {
        return xface;
    }

    /**
     * Close all the proxy objects which have been opened over the lifetime of
     * this proxy provider.
     */
    @Override
    public synchronized void close() throws IOException {
        for (AddressRpcProxyPair<T> proxy : proxies) {
            if (proxy.namenode != null) {
                if (proxy.namenode instanceof Closeable) {
                    ((Closeable) proxy.namenode).close();
                } else {
                    RPC.stopProxy(proxy.namenode);
                }
                proxy.namenode = null;
            }
        }
        proxies.clear();
    }

    protected void updateProxies(List<ActiveNode> anl) throws IOException {
        if (anl != null) {
            this.close(); // close existing proxies
            int index = 0;
            for (ActiveNode node : anl) {
                AddressRpcProxyPair<T> pair =
                        new AddressRpcProxyPair<T>(node.getRpcServerAddressForClients(),
                                index);
                proxies.add(pair);

                if(!proxiesByDomainId.containsKey(node.getLocationDomainId())){
                    proxiesByDomainId.put(node.getLocationDomainId(),
                            new ArrayList<AddressRpcProxyPair<T>>());
                }
                proxiesByDomainId.get(node.getLocationDomainId()).add(pair);
                index++;
            }

            LOG.debug(name+" new set of proxies are: "+ Arrays.toString(anl.toArray()));
        } else {
            LOG.warn(name+" no new namenodes were found");
        }
    }
}