package com.test;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.Materializer;
import akka.stream.SinkRef;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.StreamRefs;

import java.util.concurrent.CompletionStage;

public class SinkRemoteActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Materializer mat = ActorMaterializer.create(getContext());
    private Sink<String, NotUsed> sink;

    public static Props props() {
        return Props.create(SinkRemoteActor.class);
    }

    @Override
    public void preStart() throws Exception {
        sink = Flow.of(String.class)
                .log("Downstream")
                .withAttributes(Attributes.createLogLevels(
                        Logging.InfoLevel(),    //onElement
                        Logging.InfoLevel(),    //onFinish
                        Logging.ErrorLevel()    //onFailure
                )).to(Sink.ignore());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RequestRemoteStreamRef.class, m -> respondWithSinkRef())
                .build();
    }

    private void respondWithSinkRef() {
        log.info("Received request for the sink ref");

        CompletionStage<SinkRef<String>> futureSinkRef = StreamRefs.<String>sinkRef().to(sink).run(mat);
        PatternsCS.pipe(futureSinkRef.thenApply(Messages.SinkReady::new), context().dispatcher()).to(sender());
    }
}
