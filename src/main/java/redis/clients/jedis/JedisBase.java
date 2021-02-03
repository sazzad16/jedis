package redis.clients.jedis;

import java.io.Closeable;
import redis.clients.jedis.args.FlushMode;

import redis.clients.jedis.commands.BasicCommands;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;

public class JedisBase<T extends JedisBase> implements BasicCommands, Closeable {

  protected static final byte[][] DUMMY_ARRAY = new byte[0][];

  protected final Client client;
  protected Transaction transaction = null;
  protected Pipeline pipeline = null;

  private Pool<T> dataSource = null;

  public JedisBase(Client client) {
    this.client = client;
  }

  public JedisBase() {
    client = new Client();
  }

  public JedisBase(final HostAndPort hp) {
    this(hp.getHost(), hp.getPort());
  }

  public JedisBase(final HostAndPort hp, final JedisClientConfig config) {
    client = new Client(hp, config);
  }

  public JedisBase(final String host, final int port) {
    client = new Client(host, port);
  }

  public JedisBase(final JedisSocketFactory jedisSocketFactory) {
    client = new Client(jedisSocketFactory);
  }

  public boolean isBroken() {
    return client.isBroken();
  }

  @Override
  public void close() {
    if (dataSource != null) {
      unsetDataSource();
    } else {
      client.close();
    }
  }

  protected void unsetDataSource() {
    Pool<T> pool = this.dataSource;
    if (pool != null) {
      this.dataSource = null;
      if (isBroken()) {
        pool.returnBrokenResource((T) this);
      } else {
        pool.returnResource((T) this);
      }
    }
  }

  protected void setDataSource(Pool<T> jedisPool) {
    if (jedisPool != null) {
      if (dataSource != null) {
        throw new JedisException("Data source is already set.");
      }
      this.dataSource = jedisPool;
      return;
    }
    throw new JedisException("Could not set data source.");
  }

  /**
   * @return <code>PONG</code>
   */
  @Override
  public String ping() {
    checkIsInMultiOrPipeline();
    client.ping();
    return client.getStatusCodeReply();
  }

  /**
   * Ask the server to silently close the connection.
   */
  @Override
  public String quit() {
    checkIsInMultiOrPipeline();
    client.quit();
    String quitReturn = client.getStatusCodeReply();
    client.close();
    return quitReturn;
  }

  /**
   * Delete all the keys of the currently selected DB. This command never fails.
   * @return Status code reply
   */
  @Override
  public String flushDB() {
    checkIsInMultiOrPipeline();
    client.flushDB();
    return client.getStatusCodeReply();
  }

  /**
   * Delete all the keys of the currently selected DB. This command never fails.
   * @param flushMode
   * @return Status code reply
   */
  @Override
  public String flushDB(FlushMode flushMode) {
    checkIsInMultiOrPipeline();
    client.flushDB(flushMode);
    return client.getStatusCodeReply();
  }

  /**
   * Return the number of keys in the currently selected database.
   * @return Integer reply
   */
  @Override
  public Long dbSize() {
    checkIsInMultiOrPipeline();
    client.dbSize();
    return client.getIntegerReply();
  }

  /**
   * Select the DB with having the specified zero-based numeric index. For default every new client
   * connection is automatically selected to DB 0.
   * @param index
   * @return Status code reply
   */
  @Override
  public String select(final int index) {
    checkIsInMultiOrPipeline();
    client.select(index);
    String statusCodeReply = client.getStatusCodeReply();
    client.setDb(index);

    return statusCodeReply;
  }

  @Override
  public String swapDB(final int index1, final int index2) {
    checkIsInMultiOrPipeline();
    client.swapDB(index1, index2);
    return client.getStatusCodeReply();
  }

  /**
   * Delete all the keys of all the existing databases, not just the currently selected one. This
   * command never fails.
   * @return Status code reply
   */
  @Override
  public String flushAll() {
    checkIsInMultiOrPipeline();
    client.flushAll();
    return client.getStatusCodeReply();
  }

  /**
   * Delete all the keys of all the existing databases, not just the currently selected one. This
   * command never fails.
   * @param flushMode
   * @return Status code reply
   */
  @Override
  public String flushAll(FlushMode flushMode) {
    checkIsInMultiOrPipeline();
    client.flushAll(flushMode);
    return client.getStatusCodeReply();
  }

  public Transaction multi() {
    client.multi();
    client.getOne(); // expected OK
    transaction = new Transaction(client);
    return transaction;
  }

  protected void checkIsInMultiOrPipeline() {
    if (client.isInMulti()) {
      throw new IllegalStateException(
          "Cannot use Jedis when in Multi. Please use Transaction or reset jedis state.");
    } else if (pipeline != null && pipeline.hasPipelinedResponse()) {
      throw new IllegalStateException(
          "Cannot use Jedis when in Pipeline. Please use Pipeline or reset jedis state.");
    }
  }

  public void connect() {
    client.connect();
  }

  public void disconnect() {
    client.disconnect();
  }

  public void resetState() {
    if (client.isConnected()) {
      if (transaction != null) {
        transaction.close();
      }

      if (pipeline != null) {
        pipeline.close();
      }

      client.resetState();
    }

    transaction = null;
    pipeline = null;
  }

  /**
   * Request for authentication in a password protected Redis server. A Redis server can be
   * instructed to require a password before to allow clients to issue commands. This is done using
   * the requirepass directive in the Redis configuration file. If the password given by the client
   * is correct the server replies with an OK status code reply and starts accepting commands from
   * the client. Otherwise an error is returned and the clients needs to try a new password. Note
   * that for the high performance nature of Redis it is possible to try a lot of passwords in
   * parallel in very short time, so make sure to generate a strong and very long password so that
   * this attack is infeasible.
   * @param password
   * @return Status code reply
   */
  @Override
  public String auth(final String password) {
    checkIsInMultiOrPipeline();
    client.auth(password);
    return client.getStatusCodeReply();
  }

  /**
   * Request for authentication with a Redis Server that is using ACL where user are authenticated with
   * username and password.
   * See https://redis.io/topics/acl
   * @param user
   * @param password
   * @return
   */
  @Override
  public String auth(final String user, final String password) {
    checkIsInMultiOrPipeline();
    client.auth(user, password);
    return client.getStatusCodeReply();
  }

  /**
   * Synchronously save the DB on disk.
   * <p>
   * Save the whole dataset on disk (this means that all the databases are saved, as well as keys
   * with an EXPIRE set (the expire is preserved). The server hangs while the saving is not
   * completed, no connection is served in the meanwhile. An OK code is returned when the DB was
   * fully stored in disk.
   * <p>
   * The background variant of this command is {@link #bgsave() BGSAVE} that is able to perform the
   * saving in the background while the server continues serving other clients.
   * <p>
   * @return Status code reply
   */
  @Override
  public String save() {
    client.save();
    return client.getStatusCodeReply();
  }

  /**
   * Asynchronously save the DB on disk.
   * <p>
   * Save the DB in background. The OK code is immediately returned. Redis forks, the parent
   * continues to server the clients, the child saves the DB on disk then exit. A client my be able
   * to check if the operation succeeded using the LASTSAVE command.
   * @return Status code reply
   */
  @Override
  public String bgsave() {
    client.bgsave();
    return client.getStatusCodeReply();
  }

  /**
   * Rewrite the append only file in background when it gets too big. Please for detailed
   * information about the Redis Append Only File check the <a
   * href="http://redis.io/topics/persistence#append-only-file">Append Only File Howto</a>.
   * <p>
   * BGREWRITEAOF rewrites the Append Only File in background when it gets too big. The Redis Append
   * Only File is a Journal, so every operation modifying the dataset is logged in the Append Only
   * File (and replayed at startup). This means that the Append Only File always grows. In order to
   * rebuild its content the BGREWRITEAOF creates a new version of the append only file starting
   * directly form the dataset in memory in order to guarantee the generation of the minimal number
   * of commands needed to rebuild the database.
   * <p>
   * @return Status code reply
   */
  @Override
  public String bgrewriteaof() {
    client.bgrewriteaof();
    return client.getStatusCodeReply();
  }

  /**
   * Return the UNIX time stamp of the last successfully saving of the dataset on disk.
   * <p>
   * Return the UNIX TIME of the last DB save executed with success. A client may check if a
   * {@link #bgsave() BGSAVE} command succeeded reading the LASTSAVE value, then issuing a BGSAVE
   * command and checking at regular intervals every N seconds if LASTSAVE changed.
   * @return Integer reply, specifically an UNIX time stamp.
   */
  @Override
  public Long lastsave() {
    client.lastsave();
    return client.getIntegerReply();
  }

  /**
   * Synchronously save the DB on disk, then shutdown the server.
   * <p>
   * Stop all the clients, save the DB, then quit the server. This commands makes sure that the DB
   * is switched off without the lost of any data. This is not guaranteed if the client uses simply
   * {@link #save() SAVE} and then {@link #quit() QUIT} because other clients may alter the DB data
   * between the two commands.
   * @return Status code reply on error. On success nothing is returned since the server quits and
   *         the connection is closed.
   */
  @Override
  public String shutdown() {
    client.shutdown();
    String status;
    try {
      status = client.getStatusCodeReply();
    } catch (JedisException ex) {
      status = null;
    }
    return status;
  }

  /**
   * Provide information and statistics about the server.
   * <p>
   * The info command returns different information and statistics about the server in an format
   * that's simple to parse by computers and easy to read by humans.
   * <p>
   * <b>Format of the returned String:</b>
   * <p>
   * All the fields are in the form field:value
   * 
   * <pre>
   * edis_version:0.07
   * connected_clients:1
   * connected_slaves:0
   * used_memory:3187
   * changes_since_last_save:0
   * last_save_time:1237655729
   * total_connections_received:1
   * total_commands_processed:1
   * uptime_in_seconds:25
   * uptime_in_days:0
   * </pre>
   * 
   * <b>Notes</b>
   * <p>
   * used_memory is returned in bytes, and is the total number of bytes allocated by the program
   * using malloc.
   * <p>
   * uptime_in_days is redundant since the uptime in seconds contains already the full uptime
   * information, this field is only mainly present for humans.
   * <p>
   * changes_since_last_save does not refer to the number of key changes, but to the number of
   * operations that produced some kind of change in the dataset.
   * <p>
   * @return Bulk reply
   */
  @Override
  public String info() {
    client.info();
    return client.getBulkReply();
  }

  @Override
  public String info(final String section) {
    client.info(section);
    return client.getBulkReply();
  }

  /**
   * Dump all the received requests in real time.
   * <p>
   * MONITOR is a debugging command that outputs the whole sequence of commands received by the
   * Redis server. is very handy in order to understand what is happening into the database. This
   * command is used directly via telnet.
   * @param jedisMonitor
   */
  public void monitor(final JedisMonitor jedisMonitor) {
    client.monitor();
    client.getStatusCodeReply();
    jedisMonitor.proceed(client);
  }

  /**
   * Change the replication settings.
   * <p>
   * The SLAVEOF command can change the replication settings of a slave on the fly. If a Redis
   * server is already acting as slave, the command SLAVEOF NO ONE will turn off the replication
   * turning the Redis server into a MASTER. In the proper form SLAVEOF hostname port will make the
   * server a slave of the specific server listening at the specified hostname and port.
   * <p>
   * If a server is already a slave of some master, SLAVEOF hostname port will stop the replication
   * against the old server and start the synchronization against the new one discarding the old
   * dataset.
   * <p>
   * The form SLAVEOF no one will stop replication turning the server into a MASTER but will not
   * discard the replication. So if the old master stop working it is possible to turn the slave
   * into a master and set the application to use the new master in read/write. Later when the other
   * Redis server will be fixed it can be configured in order to work as slave.
   * <p>
   * @param host
   * @param port
   * @return Status code reply
   */
  @Override
  public String slaveof(final String host, final int port) {
    client.slaveof(host, port);
    return client.getStatusCodeReply();
  }

  @Override
  public String slaveofNoOne() {
    client.slaveofNoOne();
    return client.getStatusCodeReply();
  }

  /**
   * Reset the stats returned by INFO
   * @return
   */
  @Override
  public String configResetStat() {
    checkIsInMultiOrPipeline();
    client.configResetStat();
    return client.getStatusCodeReply();
  }

  /**
   * The CONFIG REWRITE command rewrites the redis.conf file the server was started with, applying
   * the minimal changes needed to make it reflect the configuration currently used by the server,
   * which may be different compared to the original one because of the use of the CONFIG SET command.
   * 
   * The rewrite is performed in a very conservative way:
   * <ul>
   * <li>Comments and the overall structure of the original redis.conf are preserved as much as possible.</li>
   * <li>If an option already exists in the old redis.conf file, it will be rewritten at the same position (line number).</li>
   * <li>If an option was not already present, but it is set to its default value, it is not added by the rewrite process.</li>
   * <li>If an option was not already present, but it is set to a non-default value, it is appended at the end of the file.</li>
   * <li>Non used lines are blanked. For instance if you used to have multiple save directives, but
   * the current configuration has fewer or none as you disabled RDB persistence, all the lines will be blanked.</li>
   * </ul>
   * 
   * CONFIG REWRITE is also able to rewrite the configuration file from scratch if the original one
   * no longer exists for some reason. However if the server was started without a configuration
   * file at all, the CONFIG REWRITE will just return an error.
   * @return OK when the configuration was rewritten properly. Otherwise an error is returned.
   */
  @Override
  public String configRewrite() {
    checkIsInMultiOrPipeline();
    client.configRewrite();
    return client.getStatusCodeReply();
  }

  public boolean isConnected() {
    return client.isConnected();
  }

  @Override
  public String debug(final DebugParams params) {
    client.debug(params);
    return client.getStatusCodeReply();
  }

  public Client getClient() {
    return client;
  }

  @Override
  public int getDB() {
    return client.getDB();
  }

  public String asking() {
    checkIsInMultiOrPipeline();
    client.asking();
    return client.getStatusCodeReply();
  }

  public String clientSetname(final String name) {
    checkIsInMultiOrPipeline();
    client.clientSetname(name);
    return client.getStatusCodeReply();
  }

  /**
   * Syncrhonous replication of Redis as described here: http://antirez.com/news/66 Since Java
   * Object class has implemented "wait" method, we cannot use it, so I had to change the name of
   * the method. Sorry :S
   */
  @Override
  public Long waitReplicas(final int replicas, final long timeout) {
    checkIsInMultiOrPipeline();
    client.waitReplicas(replicas, timeout);
    return client.getIntegerReply();
  }

  public Object sendCommand(ProtocolCommand cmd) {
    return sendCommand(cmd, DUMMY_ARRAY);
  }

  public Object sendCommand(ProtocolCommand cmd, byte[]... args) {
    checkIsInMultiOrPipeline();
    client.sendCommand(cmd, args);
    return client.getOne();
  }

  public Object sendCommand(ProtocolCommand cmd, String... args) {
    checkIsInMultiOrPipeline();
    client.sendCommand(cmd, args);
    return client.getOne();
  }

  public Object sendBlockingCommand(ProtocolCommand cmd, byte[]... args) {
    checkIsInMultiOrPipeline();
    client.sendCommand(cmd, args);
    client.setTimeoutInfinite();
    try {
      return client.getOne();
    } finally {
      client.rollbackTimeout();
    }
  }

  public Object sendBlockingCommand(ProtocolCommand cmd, String... args) {
    checkIsInMultiOrPipeline();
    client.sendCommand(cmd, args);
    client.setTimeoutInfinite();
    try {
      return client.getOne();
    } finally {
      client.rollbackTimeout();
    }
  }
}
