package com.test;

import akka.stream.SinkRef;
import akka.stream.SourceRef;

import java.io.Serializable;

public class Messages {
    private Messages() {};

    public static class RequestRemoteStreamRef implements Serializable {}

    public static class SinkReady implements Serializable {
        private final SinkRef<String> sinkRef;

        public SinkReady(SinkRef<String> sinkRef) {
            this.sinkRef = sinkRef;
        }

        public SinkRef<String> getSinkRef() {
            return sinkRef;
        }
    }

    public static class SourceReady implements Serializable {
        private final SourceRef<String> sourceRef;

        public SourceReady(SourceRef<String> sourceRef) {
            this.sourceRef = sourceRef;
        }

        public SourceRef<String> getSourceRef() {
            return sourceRef;
        }
    }
}
