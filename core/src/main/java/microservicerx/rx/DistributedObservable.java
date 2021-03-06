/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.rx;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import microservicerx.function.Serializer;
import rx.Observable;
import rx.Observer;
import rx.subjects.BehaviorSubject;

/**
 *
 * @author Oliver Rolle <oliver.rolle@the-urban-institute.de>
 */
public class DistributedObservable {

  public String address;

  public DistributedObservable() {
    this(UUID.randomUUID().toString());
  }

  public DistributedObservable(DistributedObservable that) {
    this(that.address);
  }

  public DistributedObservable(JsonObject that) {
    this(that.getString("address", null));
  }

  public DistributedObservable(String dhtAddress) {
    this.address = dhtAddress;
  }

  public JsonObject toJsonObject() {
    return new JsonObject().
      put("address", address);
  }

  public static DistributedObservable fromJsonObject(JsonObject json) {
    return new DistributedObservable(json);
  }

  public <T> Observable<T> toObservable(Vertx vertx) {
    BehaviorSubject result = BehaviorSubject.create();
    String consumerAddr = UUID.randomUUID().toString();
    AtomicReference<MessageConsumer<Object>> consumer = new AtomicReference<>(vertx.eventBus().consumer(consumerAddr));
    AtomicLong currentId = new AtomicLong(0L);

    Runnable consumerClose = () -> {
      if (consumer.get() != null) {
        consumer.get().unregister();
        consumer.set(null);
      }
    };

    consumer.get().toObservable().
      doOnNext(msg -> {
        String type = msg.headers().get(MessageHeader.ACTION.name());
        if (MessageType.NEXT.name().equals(type)) {
          Long id = -1L;
          try {
            id = Long.parseLong(msg.headers().get(MessageHeader.ID.name()));

            if (id.equals(currentId.get())) {
              currentId.incrementAndGet();
            } else {
              throw new OutOfOrderException("Out of order event of NEXT message detected! expected=" + currentId + " but was=" + id);
            }
          } catch (Exception e) {
            currentId.set(id);
            currentId.incrementAndGet();

            result.onError(e);
            return;
          }

          result.onNext(msg.body());
        } else if (MessageType.ERROR.name().equals(type)) {
          String error = msg.body() == null ? "message error is null" : msg.body().toString();
          result.onError(new IllegalStateException(error));
          
        } else if (MessageType.COMPLETED.name().equals(type)) {
          Long id = -1L;
          try {
            id = Long.parseLong(msg.headers().get(MessageHeader.ID.name()));

            if (!id.equals(currentId.get())) {
              throw new OutOfOrderException("Out of order COMPLETED detected! expected=" + currentId + " but was=" + id);
            }
          } catch (Exception e) {
            result.onError(e);
            return;
          }
          result.onCompleted();
        } else {
          result.onError(new IllegalStateException("MessageType unknown type=" + type));
        }
      }).
      doOnCompleted(result::onCompleted).
      doOnError(result::onError).
      subscribe();

    vertx.eventBus().sendObservable(address, consumerAddr).
      doOnNext(msg -> result.onError(new IllegalStateException("Received not expected msg.body()=" + msg.body()))).
      doOnCompleted(result::onCompleted).
      doOnError(result::onError).
      subscribe();

    return result.
      doOnCompleted(() -> consumerClose.run()).
      doOnError(e -> consumerClose.run()).
      cache();
  }

  public static DistributedConsumer toSendable(Observable<? extends Object> in, Vertx vertx) {
    DistributedConsumer result = new DistributedConsumer();
    result.distributed = new DistributedObservable();

    result.consumer = vertx.eventBus().consumer(result.distributed.address);
    result.consumer.toObservable().
      subscribe(msg -> {
        result.consumer.unregister();
        in.subscribe(new ObservableToEventbus(vertx, msg.body()));
      });

    return result;
  }

  public static DistributedConsumer toPublishable(Observable<? extends Object> in, Vertx vertx) {
    DistributedConsumer result = new DistributedConsumer();
    result.distributed = new DistributedObservable();

    result.consumer = vertx.eventBus().consumer(result.distributed.address);
    result.consumer.toObservable().
      subscribe(msg -> {
        in.subscribe(new ObservableToEventbus(vertx, msg.body()));
      });

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DistributedObservable) {
      DistributedObservable that = (DistributedObservable) obj;
      return Objects.equals(this.address, that.address);
    }
    return false;
  }

  @Override
  public DistributedObservable clone() {
    return new DistributedObservable(this.address);
  }

  public static String stackTraceAsString(Throwable e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString(); // stack trace as a string
  }
}

enum MessageHeader {
  ACTION, ID;
}

enum MessageType {
  NEXT, COMPLETED, ERROR;
}

class ObservableToEventbus implements Observer<Object> {

  public final Vertx vertx;
  public final String addr;
  public Long currentId;

  public ObservableToEventbus(Vertx v, String addr, Long id) {
    this.vertx = v;
    this.addr = addr;
    this.currentId = id;
  }

  public ObservableToEventbus(Vertx v, String addr) {
    this(v, addr, 0L);
  }

  @Override
  public void onCompleted() {
    vertx.eventBus().publish(addr, null, new DeliveryOptions().
      addHeader(MessageHeader.ACTION.name(), MessageType.COMPLETED.name()).
      addHeader(MessageHeader.ID.name(), currentId.toString()));
  }

  @Override
  public void onError(Throwable e) {
    String stackTrace = DistributedObservable.stackTraceAsString(e);
    vertx.eventBus().publish(addr, stackTrace,
      new DeliveryOptions().addHeader(MessageHeader.ACTION.name(), MessageType.ERROR.name()));
  }

  @Override
  public void onNext(Object t) {
    vertx.eventBus().publish(addr, t,
      new DeliveryOptions().
      addHeader(MessageHeader.ACTION.name(), MessageType.NEXT.name()).
      addHeader(MessageHeader.ID.name(), currentId.toString()));

    currentId += 1;
  }
}
