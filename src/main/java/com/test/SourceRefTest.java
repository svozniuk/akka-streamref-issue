package com.test;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.SinkRef;
import akka.stream.SourceRef;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class SourceRefTest {

    private static final Attributes logLevels = Attributes.createLogLevels(
            Logging.InfoLevel(),    //onElement
            Logging.InfoLevel(),    //onFinish
            Logging.ErrorLevel()    //onFailure
    );

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("SimpleSystem", ConfigFactory.load("no_cluster.conf"));
        ActorMaterializer mat = ActorMaterializer.create(system);
        ActorRef receiver = system.actorOf(Props.create(DataPublisher.class), "dataPublisher");

        CompletionStage<Object> ask = PatternsCS.ask(receiver, new PrepareDownload("system-42-tmp"), 5000);
        ask.thenApply(el -> {
            Sink<String, NotUsed> sink = Sink.<String>ignore().mapMaterializedValue(done -> NotUsed.getInstance());
            return ((DownloadReady)el).sourceRef.getSource()
                            .log("Downstream")
                            .withAttributes(logLevels)
                            .runWith(sink, mat);
        });
    }


    static class PrepareDownload {
        final String id;

        public PrepareDownload(String id) {
            this.id = id;
        }
    }
    static class DownloadReady {
        final String id;
        final SourceRef<String> sourceRef;

        public DownloadReady(String id, SourceRef<String> ref) {
            this.id = id;
            this.sourceRef = ref;
        }
    }

    static class DataPublisher extends AbstractActor {
        final ActorMaterializer mat = ActorMaterializer.create(getContext());

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PrepareDownload.class, prepare -> {
                        Source<String, NotUsed> source = dataSource(prepare.id);
                        CompletionStage<SourceRef<String>> sinkRef = source.runWith(StreamRefs.sourceRef(), mat);

                        PatternsCS.pipe(sinkRef.thenApply(ref -> new DownloadReady(prepare.id, ref)), context().dispatcher())
                                .to(sender());
                    })
                    .build();
        }

        private Source<String, NotUsed> dataSource(String id) {
            return Source.repeat("hello").throttle(1, Duration.ofMillis(1000))
                    .log("Upstream " + id).withAttributes(logLevels);
        }
    }
}
