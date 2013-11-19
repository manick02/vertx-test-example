package muraken720.vertx.mod.testexample;

import java.util.concurrent.ConcurrentMap;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class BusModVerticle extends Verticle {

  public void start() {

    vertx.eventBus().registerHandler("muraken720.vertx.mod.testexample",
        new Handler<Message<JsonObject>>() {
          @Override
          public void handle(Message<JsonObject> message) {
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
