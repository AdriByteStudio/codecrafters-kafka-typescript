import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  let buffer = Buffer.alloc(0);

  connection.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);

    if (buffer.length < 12) {
      return;
    }

    const version = buffer.readInt16BE(6);
    const correlationId = buffer.readInt32BE(8);

    if (version < 0 || version > 4) {
      const response = Buffer.alloc(10);
      response.writeUInt32BE(6, 0);
      response.writeInt32BE(correlationId, 4);
      response.writeInt16BE(35, 8);
      connection.end(response);
      return;
    }

    // ApiVersions v4 response body:
    // error_code (2) + api_keys compact array (1 + 2 + 2 + 2 + 1) + throttle_time_ms (4) + tag buffer (1)
    const apiKeyEntry = Buffer.alloc(9);
    apiKeyEntry.writeInt16BE(18, 0); // API key 18 (ApiVersions)
    apiKeyEntry.writeInt16BE(0, 2); // min_version
    apiKeyEntry.writeInt16BE(4, 4); // max_version
    apiKeyEntry[6] = 0; // TAG_BUFFER empty

    const body = Buffer.alloc(2 + 1 + apiKeyEntry.length + 4 + 1);
    let offset = 0;

    body.writeInt16BE(0, offset);
    offset += 2;

    body[offset] = 2; // compact array length: 1 element
    offset += 1;

    apiKeyEntry.copy(body, offset);
    offset += apiKeyEntry.length;

    body.writeInt32BE(0, offset);
    offset += 4;

    body[offset] = 0; // final TAG_BUFFER empty

    const response = Buffer.alloc(4 + 4 + body.length);
    response.writeUInt32BE(4 + body.length, 0);
    response.writeInt32BE(correlationId, 4);
    body.copy(response, 8);

    connection.end(response);
  });
});

server.listen(9092, "127.0.0.1");
