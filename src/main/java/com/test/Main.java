package com.test;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Please specify mode");
        }

        switch (args[0]) {
            case "source":
                startSource();
                break;
            case "sink":
                startSink();
                break;
            default:
                log.error("Only Source and Sink modes supported");
                throw new IllegalArgumentException("Invalid mode");
        }
    }

    private static void startSink() {
        Config config = ConfigFactory.load("sink");
        ActorSystem system = ActorSystem.create("ClusterTesting", config);
        system.actorOf(SinkRemoteActor.props(), "sink");
    }

    private static void startSource() {
        Config config = ConfigFactory.load("source");
        ActorSystem system = ActorSystem.create("ClusterTesting", config);
        system.actorOf(SourceOriginActor.props(), "source");
    }

}
