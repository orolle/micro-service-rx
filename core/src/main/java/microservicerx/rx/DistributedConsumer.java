/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package microservicerx.rx;

import io.vertx.rxjava.core.eventbus.MessageConsumer;

/**
 *
 * @author Oliver Rolle
 */
public class DistributedConsumer {
  public DistributedObservable distributed;
  public MessageConsumer<String> consumer;
}
