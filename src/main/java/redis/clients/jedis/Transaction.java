package redis.clients.jedis;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Transaction is nearly identical to Pipeline, only differences are the multi/discard behaviors
 */
public class Transaction<J extends JedisBase> extends MultiKeyPipelineBase<J> implements Closeable {

  /**
   * @deprecated This will be private in future.
   */
  @Deprecated
  protected boolean inTransaction = true;

  private int pendingResponses = 0;

  /**
   * @deprecated This constructor will be removed in future.
   */
  @Deprecated
  protected Transaction() {
    super(null);
    // client will be set later in transaction block
  }

  /**
   * @deprecated This constructor will be removed in future.
   * @param client
   */
  @Deprecated
  public Transaction(final Client client) {
    super(null);
    setClient(client);
  }

  public Transaction(J resource) {
    super(resource);
    getClient().multi();
    ++pendingResponses;
//    getResponse(BuilderFactory.STRING);
  }

  @Override
  public void clear() {
    if (inTransaction) {
      discard();
    }
  }

  public List<Object> exec() {
    // Discard QUEUED or ERROR
    getClient().getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    getClient().exec();
    inTransaction = false;

    List<Object> unformatted = getClient().getObjectMultiBulkReply();
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
    getClient().getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    getClient().exec();
    inTransaction = false;

    List<Object> unformatted = getClient().getObjectMultiBulkReply();
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
    getClient().getMany(pendingResponses + getPipelinedResponseLength());
    pendingResponses = 0;
    getClient().discard();
    inTransaction = false;
    clean();
    return getClient().getStatusCodeReply();
  }
}