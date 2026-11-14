// THE DESK'S ENVIRONMENTS AT A TEST'S SERVER (docs/ONE_CONNECTION_DESK_2026_10_28.md D2): a site or a lane that starts
// a Deephaven server of its own (at a port of its own) serves its copy of the desk's environments.pure, its Deephaven
// context at that server -- a test's input, never the platform's. Product code reads the file as it is.

/** The desk's environments (environments.pure) with its Deephaven context at `server` (a test's copy: the server the test
 *  started is at a port of its own); refused when the file names no Deephaven server where the desk's does. */
export function environmentsAt(environments, server, door) {
  const at = "host: 'localhost'; port: 10000;";
  if (!environments.includes(at)) throw new Error(`the desk's environments name no Deephaven server at ${at}`);
  const brokers = "bootstrapServers: '127.0.0.1:19092';";
  if (door !== undefined && !environments.includes(brokers)) {
    throw new Error(`the desk's environments name no topic brokers at ${brokers}`);
  }
  const moved = environments.replaceAll(at, `host: '${server.hostname}'; port: ${Number(server.port)};`)
    .replaceAll('localhost:10000', `${server.hostname}:${Number(server.port)}`);
  // the door a test started, at a port of its own (B2: the server's startup program subscribes there)
  return door === undefined ? moved : moved.replaceAll(brokers, `bootstrapServers: '127.0.0.1:${Number(door)}';`);
}
