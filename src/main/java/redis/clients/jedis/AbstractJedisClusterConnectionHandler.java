package redis.clients.jedis;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.util.SafeEncoder;

public abstract class AbstractJedisClusterConnectionHandler<J extends JedisBase,
    P extends JedisPoolBase<J>> implements Closeable {

  private final Set<HostAndPort> seedNodes;

  private final Map<String, P> nodes = new HashMap<>();
  private final Map<Integer, P> slots = new HashMap<>();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
  private final Lock r = rwl.readLock();
  private final Lock w = rwl.writeLock();
  private volatile boolean rediscovering;

  private static final int MASTER_NODE_INDEX = 2;

  public AbstractJedisClusterConnectionHandler(Set<HostAndPort> nodes) {
    this.seedNodes = nodes;
  }

  protected final void initializeSlotsCache() {
    for (HostAndPort hostAndPort : seedNodes) {
      try (J jedis = createSeedConnection(hostAndPort)) {
        discoverClusterNodesAndSlots(jedis);
        return;
      } catch (JedisConnectionException e) {
        // try next nodes
      }
    }
  }

  protected abstract J createSeedConnection(HostAndPort hostAndPort);

  public J getConnection() {
    // In antirez's redis-rb-cluster implementation, getRandomConnection always return valid
    // connection (able to ping-pong) or exception if all connections are invalid

    List<P> pools = getShuffledNodesPool();

    JedisException suppressed = null;
    for (P pool : pools) {
      J jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        if (jedis.ping().equalsIgnoreCase("pong")) {
          return jedis;
        }

        jedis.close();
      } catch (JedisException ex) {
        if (suppressed == null) { // remembering first suppressed exception
          suppressed = ex;
        }
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    JedisNoReachableClusterNodeException noReachableNode = new JedisNoReachableClusterNodeException("No reachable node in cluster.");
    if (suppressed != null) {
      noReachableNode.addSuppressed(suppressed);
    }
    throw noReachableNode;
  }

  public J getConnectionFromSlot(int slot) {
    P connectionPool = getSlotPool(slot);
    // It is not guaranteed to get valid connection because of node assignment
    if (connectionPool != null) {
      return connectionPool.getResource();
    } else {
      // It's abnormal situation for cluster mode, that we have just nothing for slot, try to
      // rediscover state
      renewSlotCache();
      connectionPool = getSlotPool(slot);
      if (connectionPool != null) {
        return connectionPool.getResource();
      } else {
        //no choice, fallback to new connection to random node
        return getConnection();
      }
    }
  }

  public J getConnectionFromNode(HostAndPort node) {
    return setupNodeIfNotExist(node).getResource();
  }

  public void renewSlotCache() {
    renewClusterSlots(null);
  }

  public void renewSlotCache(J jedis) {
    renewClusterSlots(jedis);
  }

  @Override
  public void close() {
    reset();
  }

  public void discoverClusterNodesAndSlots(J jedis) {
    w.lock();

    try {
      reset();
      List<Object> slots = (List<Object>) generateClusterConfigResponse(jedis);

      for (Object slotInfoObj : slots) {
        List<Object> slotInfo = (List<Object>) slotInfoObj;

        if (slotInfo.size() <= MASTER_NODE_INDEX) {
          continue;
        }

        List<Integer> slotNums = getAssignedSlotArray(slotInfo);

        // hostInfos
        int size = slotInfo.size();
        for (int i = MASTER_NODE_INDEX; i < size; i++) {
          List<Object> hostInfos = (List<Object>) slotInfo.get(i);
          if (hostInfos.isEmpty()) {
            continue;
          }

          HostAndPort targetNode = generateHostAndPort(hostInfos);
          setupNodeIfNotExist(targetNode);
          if (i == MASTER_NODE_INDEX) {
            assignSlotsToNode(slotNums, targetNode);
          }
        }
      }
    } finally {
      w.unlock();
    }
  }

  
  public void renewClusterSlots(J jedis) {
    //If rediscovering is already in process - no need to start one more same rediscovering, just return
    if (!rediscovering) {
      try {
        w.lock();
        if (!rediscovering) {
          rediscovering = true;

          try {
            if (jedis != null) {
              try {
                discoverClusterSlots(jedis);
                return;
              } catch (JedisException e) {
                //try nodes from all pools
              }
            }

            for (P jp : getShuffledNodesPool()) {
              J j = null;
              try {
                j = jp.getResource();
                discoverClusterSlots(j);
                return;
              } catch (JedisConnectionException e) {
                // try next nodes
              } finally {
                if (j != null) {
                  j.close();
                }
              }
            }
          } finally {
            rediscovering = false;      
          }
        }
      } finally {
        w.unlock();
      }
    }
  }

  /**
   * @param jedis
   * @return Result of {@link ClusterCommands#clusterSlots()}
   */
  public abstract Object generateClusterConfigResponse(J jedis);

  private void discoverClusterSlots(J jedis) {
    List<Object> slots = (List<Object>) generateClusterConfigResponse(jedis);
    this.slots.clear();

    for (Object slotInfoObj : slots) {
      List<Object> slotInfo = (List<Object>) slotInfoObj;

      if (slotInfo.size() <= MASTER_NODE_INDEX) {
        continue;
      }

      List<Integer> slotNums = getAssignedSlotArray(slotInfo);

      // hostInfos
      List<Object> hostInfos = (List<Object>) slotInfo.get(MASTER_NODE_INDEX);
      if (hostInfos.isEmpty()) {
        continue;
      }

      // at this time, we just use master, discard slave information
      HostAndPort targetNode = generateHostAndPort(hostInfos);
      assignSlotsToNode(slotNums, targetNode);
    }
  }

  private HostAndPort generateHostAndPort(List<Object> hostInfos) {
    String host = SafeEncoder.encode((byte[]) hostInfos.get(0));
    int port = ((Long) hostInfos.get(1)).intValue();
    return new HostAndPort(host, port);
  }

  private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
    List<Integer> slotNums = new ArrayList<>();
    for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
        .intValue(); slot++) {
      slotNums.add(slot);
    }
    return slotNums;
  }

  protected abstract P createPool(HostAndPort node);

  public P setupNodeIfNotExist(final HostAndPort node) {
    w.lock();
    try {
      String nodeKey = getNodeKey(node);
      P existingPool = nodes.get(nodeKey);
      if (existingPool != null) return existingPool;

      P nodePool = (P) createPool(node);
      nodes.put(nodeKey, nodePool);
      return nodePool;
    } finally {
      w.unlock();
    }
  }

  public void assignSlotToNode(int slot, HostAndPort targetNode) {
    w.lock();
    try {
      P targetPool = setupNodeIfNotExist(targetNode);
      slots.put(slot, targetPool);
    } finally {
      w.unlock();
    }
  }

  public void assignSlotsToNode(List<Integer> targetSlots, HostAndPort targetNode) {
    w.lock();
    try {
      P targetPool = setupNodeIfNotExist(targetNode);
      for (Integer slot : targetSlots) {
        slots.put(slot, targetPool);
      }
    } finally {
      w.unlock();
    }
  }

  public P getNode(String nodeKey) {
    r.lock();
    try {
      return nodes.get(nodeKey);
    } finally {
      r.unlock();
    }
  }

  public P getSlotPool(int slot) {
    r.lock();
    try {
      return slots.get(slot);
    } finally {
      r.unlock();
    }
  }

  public Map<String, P> getNodes() {
    r.lock();
    try {
      return new HashMap<>(nodes);
    } finally {
      r.unlock();
    }
  }

  public List<P> getShuffledNodesPool() {
    r.lock();
    try {
      List<P> pools = new ArrayList<>(nodes.values());
      Collections.shuffle(pools);
      return pools;
    } finally {
      r.unlock();
    }
  }

  /**
   * Clear discovered nodes collections and gently release allocated resources
   */
  public void reset() {
    w.lock();
    try {
      for (P pool : nodes.values()) {
        try {
          if (pool != null) {
            pool.destroy();
          }
        } catch (Exception e) {
          // pass
        }
      }
      nodes.clear();
      slots.clear();
    } finally {
      w.unlock();
    }
  }

  public static String getNodeKey(HostAndPort hnp) {
    return hnp.getHost() + ":" + hnp.getPort();
  }

}
