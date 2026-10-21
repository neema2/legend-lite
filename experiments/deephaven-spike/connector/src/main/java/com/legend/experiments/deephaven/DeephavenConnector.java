package com.legend.experiments.deephaven;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.PublishRequest;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The execution side of a Deephaven target, as small as it can be: one gRPC session that runs a
 * plan and publishes its result to a shared ticket other sessions (a browser, a Python process)
 * subscribe to.
 *
 * <p>The plan is a {@code BatchTableRequest} in protobuf's JSON form: the planner writes it with no
 * Deephaven library, and this class only fills in the ticket of the last operation (tickets
 * belong to a session, so the plan never carries one). Java carries the plan and the handle; every
 * value, on every tick, is computed by Deephaven.
 */
public final class DeephavenConnector implements AutoCloseable {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String PSK_HANDLER = "io.deephaven.authentication.psk.PskAuthenticationHandler";

    /** A published live table: the shared ticket a viewer fetches, and the export that keeps it alive. */
    public record Live(String sharedId, Ticket export, boolean isStatic, long size, long publishMillis) {
    }

    private final ManagedChannel channel;
    /** The header every call carries: the PSK until the server answers with a bearer token. */
    private final AtomicReference<String> authorization;
    private final TableServiceGrpc.TableServiceBlockingStub tables;
    private final SessionServiceGrpc.SessionServiceBlockingStub sessions;
    private final ConfigServiceGrpc.ConfigServiceBlockingStub config;
    private final AtomicInteger nextExport = new AtomicInteger(1);
    private final ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "deephaven-session-refresh");
        t.setDaemon(true);
        return t;
    });

    private DeephavenConnector(String host, int port, String psk) {
        this.authorization = new AtomicReference<>(PSK_HANDLER + " " + psk);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .intercept(new BearerInterceptor())
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();
        this.tables = TableServiceGrpc.newBlockingStub(channel);
        this.sessions = SessionServiceGrpc.newBlockingStub(channel);
        this.config = ConfigServiceGrpc.newBlockingStub(channel);
    }

    /** Opens a session: the first call authenticates, and the session is refreshed at half its lifetime. */
    public static DeephavenConnector open(String host, int port, String psk) {
        DeephavenConnector c = new DeephavenConnector(host, port, psk);
        ConfigurationConstantsResponse constants = c.config.getConfigurationConstants(
                ConfigurationConstantsRequest.getDefaultInstance());
        long sessionMillis = Long.parseLong(
                constants.getConfigValuesOrThrow("http.session.durationMs").getStringValue());
        if (!c.authorization.get().startsWith("Bearer ")) {
            throw new IllegalStateException("the server answered the first call without a session token");
        }
        long every = Math.max(1000, sessionMillis / 2);
        // a throwing task would cancel every later run, so a failed refresh is reported and retried next time
        c.refresher.scheduleAtFixedRate(() -> {
            try {
                c.config.getConfigurationConstants(ConfigurationConstantsRequest.getDefaultInstance());
            } catch (RuntimeException e) {
                System.err.println("deephaven session refresh failed: " + e.getMessage());
            }
        }, every, every, TimeUnit.MILLISECONDS);
        return c;
    }

    /** Reads a plan: the {@code batch} member of a plan document, in protobuf's JSON form. */
    public static BatchTableRequest parsePlan(String batchJson) {
        BatchTableRequest.Builder b = BatchTableRequest.newBuilder();
        try {
            JsonFormat.parser().merge(batchJson, b);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("not a Deephaven batch: " + e.getMessage(), e);
        }
        if (b.getOpsCount() == 0) {
            throw new IllegalArgumentException("a plan has at least one operation");
        }
        return b.build();
    }

    /**
     * Runs a plan and publishes its result: the derived table is built once on the server and from
     * then on maintained by Deephaven's update graph; nothing here runs per tick.
     */
    public Live publish(BatchTableRequest plan, String sharedId) {
        long start = System.nanoTime();
        Ticket export = exportTicket(nextExport.getAndIncrement());
        BatchTableRequest batch = withLastResult(plan, export);
        ExportedTableCreationResponse last = null;
        List<String> errors = new ArrayList<>();
        for (Iterator<ExportedTableCreationResponse> it = tables.batch(batch); it.hasNext(); ) {
            ExportedTableCreationResponse r = it.next();
            if (!r.getSuccess()) {
                errors.add(r.getErrorInfo());
            }
            if (r.getResultId().hasTicket() && r.getResultId().getTicket().equals(export)) {
                last = r;
            }
        }
        if (!errors.isEmpty() || last == null) {
            throw new IllegalStateException("the plan failed on the server: " + errors);
        }
        sessions.publishFromTicket(PublishRequest.newBuilder()
                .setSourceId(export)
                .setResultId(sharedTicket(sharedId))
                .build());
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        return new Live(sharedId, export, last.getIsStatic(), last.getSize(), millis);
    }

    /** Releases this session's export: the shared ticket stops resolving once nothing else holds the table. */
    public void release(Live live) {
        sessions.release(ReleaseRequest.newBuilder().setId(live.export()).build());
    }

    @Override
    public void close() {
        refresher.shutdownNow();
        channel.shutdownNow();
    }

    /** The last operation's {@code result_id}: every operation message names that field the same. */
    private static BatchTableRequest withLastResult(BatchTableRequest plan, Ticket export) {
        int lastIndex = plan.getOpsCount() - 1;
        BatchTableRequest.Operation.Builder op = plan.getOps(lastIndex).toBuilder();
        OneofDescriptor oneof = BatchTableRequest.Operation.getDescriptor().getOneofs().get(0);
        FieldDescriptor which = op.getOneofFieldDescriptor(oneof);
        if (which == null) {
            throw new IllegalArgumentException("the plan's last operation is empty");
        }
        Message.Builder inner = op.getFieldBuilder(which);
        FieldDescriptor resultId = inner.getDescriptorForType().findFieldByName("result_id");
        if (resultId == null) {
            throw new IllegalArgumentException("operation '" + which.getName() + "' has no result_id");
        }
        inner.setField(resultId, export);
        return plan.toBuilder().setOps(lastIndex, op).build();
    }

    private static Ticket exportTicket(int id) {
        ByteBuffer b = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) 'e').putInt(id);
        return Ticket.newBuilder().setTicket(ByteString.copyFrom(b.array())).build();
    }

    /**
     * {@code "h/"} and the identifier's UTF-8 bytes. The JS API's {@code getSharedObject(string)} writes
     * {@code 'h'}, then {@code '/'}, then the string (web client-api {@code Tickets.sharedTicket}), while the
     * server takes every byte after {@code 'h'} as the identifier ({@code SharedTicketResolver.idForTicket})
     * and the Java and Python helpers write no {@code '/'}. Publishing with the {@code '/'} is the one
     * spelling every viewer can reach: a Python viewer asks for {@code b"h/" + id}.
     */
    static Ticket sharedTicket(String sharedId) {
        byte[] id = sharedId.getBytes(StandardCharsets.UTF_8);
        byte[] t = new byte[id.length + 2];
        t[0] = (byte) 'h';
        t[1] = (byte) '/';
        System.arraycopy(id, 0, t, 2, id.length);
        return Ticket.newBuilder().setTicket(ByteString.copyFrom(t)).build();
    }

    /** Sends the current authorization header and keeps the bearer token each response carries. */
    private final class BearerInterceptor implements ClientInterceptor {
        @Override
        public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
                @Override
                public void start(Listener<R> listener, Metadata headers) {
                    headers.put(AUTHORIZATION, authorization.get());
                    super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                        @Override
                        public void onHeaders(Metadata answered) {
                            String token = answered.get(AUTHORIZATION);
                            if (token != null && token.startsWith("Bearer ")) {
                                authorization.set(token);
                            }
                            super.onHeaders(answered);
                        }
                    }, headers);
                }
            };
        }
    }
}
