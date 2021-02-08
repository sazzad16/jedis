package redis.clients.jedis;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Transaction is nearly identical to Pipeline, only differences are the multi/discard behaviors
 */
public class Transaction<J extends JedisBase> extends MultiKeyPipelineBase implements Closeable {

  private final J resource;
  private int pendingResponses = 0;

  /**
   * @deprecated This will be private in future.
   */
  @Deprecated
  protected boolean inTransaction = true;

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  protected Transaction() {
    this.resource = null;
    // client will be set later in transaction block
  }

  /**
   * @deprecated This constructor will be removed in future.
   * @param client
   */
  @Deprecated
  public Transaction(final Client client) {
    this.resource = null;
    this.client = client;
  }

  public Transaction(J resource) {
    this.resource = resource;
    this.client = resource.getClient();
    this.client.multi();
    ++pendingResponses;
//    getResponse(BuilderFactory.STRING);
  }

  /**
   * @deprecated This will be removed in future.
   * @param client
   */
  public void setClient(Client client) {
    if (this.resource == null) {
      this.client = client;
    }
  }

  /**
   * @deprecated This will be final in future.
   * @param key
   * @return
   */
  @Deprecated
  @Override
  protected Client getClient(String key) {
    return client;
  }

  /**
   * @deprecated This will be final in future.
   * @param key
   * @return
   */
  @Deprecated
  @Override
  protected Client getClient(byte[] key) {
    return client;
  }

  public void clear() {
    if (inTransaction) {
      discard();
    }
  }

  public List<Object> exec() {
    // Discard QUEUED or ERROR
    client.getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    client.exec();
    inTransaction = false;

    List<Object> unformatted = client.getObjectMultiBulkReply();
    if (unformatted == null) {
      return null;
    }
    List<Object> formatted = new ArrayList<>();
    for (Object o : unformatted) {
      try {
        formatted.add(generateResponse(o).get());
      } catch (JedisDataException e) {
        formatted.add(e);
      }
    }
    return formatted;
  }

  public List<Response<?>> execGetResponse() {
    // Discard QUEUED or ERROR
    client.getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    client.exec();
    inTransaction = false;

    List<Object> unformatted = client.getObjectMultiBulkReply();
    if (unformatted == null) {
      return null;
    }
    List<Response<?>> response = new ArrayList<>();
    for (Object o : unformatted) {
      response.add(generateResponse(o));
    }
    return response;
  }

  public String discard() {
    client.getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    client.discard();
    inTransaction = false;
    clean();
    return client.getStatusCodeReply();
  }

  @Override
  public void close() {
    clear();
    if (this.resource != null) {
      this.resource.unsetDataSource();
    }
  }
}