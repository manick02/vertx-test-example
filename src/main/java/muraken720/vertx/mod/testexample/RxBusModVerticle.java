package muraken720.vertx.mod.testexample;

import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;

import java.util.concurrent.ConcurrentMap;

import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import rx.Observable;
import rx.util.functions.Action1;

public class RxBusModVerticle extends Verticle {

  public void start() {
    container.logger().info("RxBusModVerticle start.");

    RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

    Observable<RxMessage<JsonObject>> obs = rxEventBus
        .<JsonObject> registerHandler("muraken720.vertx.mod.testexample");

    obs.subscribe(new Action1<RxMessage<JsonObject>>() {
      @Override
      public void call(RxMessage<JsonObject> message) {
        ConcurrentMap<String, String> map = vertx.sharedData().getMap(
            "muraken720.testexample");

        JsonObject json = message.body();

        if ("add".equals(json.getString("action"))) {
          String key = json.getString("key");
          String value = json.getString("value");
          map.put(key, value);
        } else {
          message.reply(new JsonObject().putString("status", "error")
              .putString("message", "unknown action."));
        }

        message.reply(new JsonObject().putString("status", "ok"));
      }
    });
  }

}
