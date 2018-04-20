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
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class SinkRefTest {

    private static final Attributes logLevels = Attributes.createLogLevels(
            Logging.InfoLevel(),    //onElement
            Logging.InfoLevel(),    //onFinish
            Logging.ErrorLevel()    //onFailure
    );

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("SimpleSystem", ConfigFactory.load("no_cluster.conf"));
        ActorMaterializer mat = ActorMaterializer.create(system);
        ActorRef receiver = system.actorOf(Props.create(DataReceiver.class), "dataReceiver");

        CompletionStage<Object> ask = PatternsCS.ask(receiver, new PrepareUpload("system-42-tmp"), 5000);
        ask.thenApply(el -> Source.repeat("hello").throttle(1, Duration.ofMillis(1000))
                .log("Upstream").withAttributes(logLevels).runWith(((MeasurementsSinkReady)el).sinkRef.getSink(), mat));

    }


    static class PrepareUpload {
        final String id;

        public PrepareUpload(String id) {
            this.id = id;
        }
    }
    static class MeasurementsSinkReady {
        final String id;
        final SinkRef<String> sinkRef;

        public MeasurementsSinkReady(String id, SinkRef<String> ref) {
            this.id = id;
            this.sinkRef = ref;
        }
    }

    static class DataReceiver extends AbstractActor {
        final ActorMaterializer mat = ActorMaterializer.create(getContext());

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(PrepareUpload.class, prepare -> {
                        Sink<String, NotUsed> sink = logsSinkFor(prepare.id);
                        CompletionStage<SinkRef<String>> sinkRef = StreamRefs.<String>sinkRef().to(sink).run(mat);

                        PatternsCS.pipe(sinkRef.thenApply(ref -> new MeasurementsSinkReady(prepare.id, ref)), context().dispatcher())
                                .to(sender());
                    })
                    .build();
        }

        private Sink<String, NotUsed> logsSinkFor(String id) {

            return Flow.of(String.class)
                    .log("Downstream " + id)
                    .withAttributes(logLevels)
                    .to(Sink.<String>ignore().mapMaterializedValue(done -> NotUsed.getInstance()));
        }
    }
}
