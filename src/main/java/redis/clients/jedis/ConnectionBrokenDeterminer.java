package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.List;

public class ConnectionBrokenDeterminer {
  private final List<ConnectionBrokenPattern> filters;

  public ConnectionBrokenDeterminer() {
    filters = new ArrayList<ConnectionBrokenPattern>();
  }

  public ConnectionBrokenDeterminer addPattern(final ConnectionBrokenPattern pattern) {
    filters.add(pattern);
    return this;
  }

  public boolean determine(final Connection conn, final RuntimeException exc) {
    for (ConnectionBrokenPattern filter : filters) {
      if (filter.determine(conn, exc)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Add a pattern which will treat any JedisConnectionException as broken connection
   */
  public ConnectionBrokenDeterminer considerJedisConnectionExceptionAsConnectionBroken() {
    return addPattern(new JedisConnectionExceptionAsConnectionBroken());
  }

  /**
   * Add a pattern which will try a single PING. Failure will be treated as broken connection
   */
  public ConnectionBrokenDeterminer considerFailedPingAsConnectionBroken() {
    return addPattern(new FailedPingAsConnectionBroken());
  }

  /**
   * Treats JedisConnectionException as broken connection
   */
  private static class JedisConnectionExceptionAsConnectionBroken
      implements ConnectionBrokenPattern {
    @Override public boolean determine(final Connection conn, final RuntimeException exc) {
      if (exc instanceof JedisConnectionException) {
        return true;
      }
      return false;
    }
  }

  /**
   * Treats failed single ping attempt as broken connection
   */
  private static class FailedPingAsConnectionBroken implements ConnectionBrokenPattern {
    @Override public boolean determine(final Connection conn, final RuntimeException exc) {
      try {
        conn.sendCommand(Protocol.Command.PING);
        String reply = conn.getBulkReply();
        return "PONG".equals(reply);
      } catch (Exception ex) {
        // consume any sort of exception
      }
      return false;
    }
  }
}
