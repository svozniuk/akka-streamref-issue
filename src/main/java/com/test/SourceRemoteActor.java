package com.test;

import akka.NotUsed;
import akka.actor.AbstractActorWithTimers;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.PatternsCS;
import akka.stream.*;
import akka.stream.javadsl.*;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

public class SourceRemoteActor extends AbstractActorWithTimers {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private final Materializer mat = ActorMaterializer.create(getContext());
    private Source<String, NotUsed> source;

    public static Props props() {
        return Props.create(SourceRemoteActor.class);
    }

    @Override
    public void preStart() throws Exception {
        source = Source.repeat("data")
                .throttle(1, Duration.ofMillis(1000))
                .log("Upstream")
                .withAttributes(Attributes.createLogLevels(
                        Logging.InfoLevel(),    //onElement
                        Logging.InfoLevel(),    //onFinish
                        Logging.ErrorLevel()    //onFailure
                ));

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RequestRemoteStreamRef.class, m -> respondWithSourceRef())
                .build();
    }

    private void respondWithSourceRef() {
        log.info("Received request for the source ref");

        CompletionStage<SourceRef<String>> futureStreamRef = source.runWith(StreamRefs.sourceRef(), mat);
        PatternsCS.pipe(futureStreamRef.thenApply(Messages.SourceReady::new), context().dispatcher()).to(sender());
    }
}
