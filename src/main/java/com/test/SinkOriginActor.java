package com.test;

import akka.NotUsed;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.routing.ClusterRouterGroup;
import akka.cluster.routing.ClusterRouterGroupSettings;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.BroadcastGroup;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.test.SourceOriginActor.Internal.SINK_TICK;

public class SinkOriginActor extends AbstractActorWithTimers {
    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Sink<String, NotUsed> sink;
    private final Materializer mat = ActorMaterializer.create(getContext());
    private ActorRef router;

    public static Props props() {
        return Props.create(SinkOriginActor.class);
    }

    @Override
    public void preStart() throws Exception {
        router = prepareRouter("sourceFinderBroadcast");
        sink = Flow.of(String.class)
                .log("Downstream")
                .withAttributes(Attributes.createLogLevels(
                        Logging.InfoLevel(),    //onElement
                        Logging.InfoLevel(),    //onFinish
                        Logging.ErrorLevel()    //onFailure
                )).to(Sink.ignore());
        scheduleTicks();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(SINK_TICK, tick -> askForSource())
                .match(Messages.SourceReady.class, m -> connectToSource(m))
                .build();
    }

    private void askForSource() {
        log.info("Trying to discover source");
        router.tell(new Messages.RequestRemoteStreamRef(), getSelf());
    }

    private void scheduleTicks() {
        FiniteDuration logPeriod = FiniteDuration.create(5000, TimeUnit.MILLISECONDS);
        getTimers().startPeriodicTimer("find-source-timer", SINK_TICK, logPeriod);
    }

    private void connectToSource(Messages.SourceReady sourceReady) {
        log.info("Found sink {}", getSender());
        getTimers().cancel("find-source-timer");
        RunnableGraph<NotUsed> graph = sourceReady.getSourceRef().getSource().to(sink);
        graph.run(mat);
    }

    private ActorRef prepareRouter(String name) {
        Set<String> useRoles = new HashSet<>(Arrays.asList("dc-default"));
        int totalInstances = 100;
        Iterable<String> routeesPaths = Collections.singletonList("/user/source/");
        boolean allowLocalRoutees = true;
        ClusterRouterGroupSettings settings = new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles);
        BroadcastGroup policy = new BroadcastGroup(routeesPaths);
        return getContext().actorOf(new ClusterRouterGroup(policy, settings).props(), name);
    }

    enum Internal {
        SINK_TICK
    }
}
