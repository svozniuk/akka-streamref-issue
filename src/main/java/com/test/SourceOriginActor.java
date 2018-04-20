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
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.test.SourceOriginActor.Internal.SINK_TICK;

public class SourceOriginActor extends AbstractActorWithTimers {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private Source<String, NotUsed> source;
    private final Materializer mat = ActorMaterializer.create(getContext().getSystem());
    private ActorRef router;

    public static Props props() {
        return Props.create(SourceOriginActor.class);
    }


    @Override
    public void preStart() throws Exception {
        router = prepareRouter("sinkFinderBroadcast");
        source = Source.repeat("data")
                .throttle(1, Duration.ofMillis(1000))
                .log("Upstream")
                .withAttributes(Attributes.createLogLevels(
                        Logging.InfoLevel(),    //onElement
                        Logging.InfoLevel(),    //onFinish
                        Logging.ErrorLevel()    //onFailure
                ));
        scheduleTicks();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(SINK_TICK, tick -> askForSink())
                .match(Messages.SinkReady.class, m -> connectToSink(m))
                .build();
    }

    private void askForSink() {
        log.info("Trying to discover sink");
        router.tell(new Messages.RequestRemoteStreamRef(), getSelf());
    }

    private void scheduleTicks() {
        FiniteDuration logPeriod = FiniteDuration.create(5000, TimeUnit.MILLISECONDS);
        getTimers().startPeriodicTimer("find-sink-timer", SINK_TICK, logPeriod);
    }

    private void connectToSink(Messages.SinkReady sinkReady) {
        log.info("Found sink {}", getSender());
        getTimers().cancel("find-sink-timer");
        RunnableGraph<NotUsed> graph = source.to(sinkReady.getSinkRef().getSink());
        graph.run(mat);
    }

    private ActorRef prepareRouter(String name) {
        Set<String> useRoles = new HashSet<>(Arrays.asList("dc-default"));
        int totalInstances = 100;
        Iterable<String> routeesPaths = Collections.singletonList("/user/sink/");
        boolean allowLocalRoutees = true;
        ClusterRouterGroupSettings settings = new ClusterRouterGroupSettings(totalInstances, routeesPaths, allowLocalRoutees, useRoles);
        BroadcastGroup policy = new BroadcastGroup(routeesPaths);
        return getContext().actorOf(new ClusterRouterGroup(policy, settings).props(), name);
    }

    enum Internal {
        SINK_TICK
    }
}
