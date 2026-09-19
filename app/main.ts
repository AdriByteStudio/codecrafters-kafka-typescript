import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  let buffer = Buffer.alloc(0);

  connection.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 4) {
      const messageSize = buffer.readUInt32BE(0);
      let messageLength = 4 + messageSize;

      if (buffer.length >= 14) {
        const clientIdLength = buffer.readInt16BE(12);
        const clientIdSize = clientIdLength < 0 ? 0 : clientIdLength;
        const bodyOffset = 14 + clientIdSize + 1;

        if (buffer.length > bodyOffset) {
          const clientSoftwareNameLength = buffer[bodyOffset];
          const versionLengthOffset = bodyOffset + clientSoftwareNameLength;

          if (buffer.length > versionLengthOffset) {
            const clientSoftwareVersionLength = buffer[versionLengthOffset];
            const requestEnd = versionLengthOffset + clientSoftwareVersionLength + 1;
            messageLength = Math.max(messageLength, requestEnd);
          }
        }
      }

      if (buffer.length < messageLength) {
        return;
      }

      const request = buffer.subarray(0, messageLength);
      buffer = buffer.subarray(messageLength);

      const version = request.readInt16BE(6);
      const correlationId = request.readInt32BE(8);

      if (version < 0 || version > 4) {
        const response = Buffer.alloc(10);
        response.writeUInt32BE(6, 0);
        response.writeInt32BE(correlationId, 4);
        response.writeInt16BE(35, 8);
        connection.write(response);
        continue;
      }

      const apiKeys = Buffer.alloc(14);
      apiKeys.writeInt16BE(18, 0);
      apiKeys.writeInt16BE(0, 2);
      apiKeys.writeInt16BE(4, 4);
      apiKeys.writeInt16BE(75, 7);
      apiKeys.writeInt16BE(0, 9);
      apiKeys.writeInt16BE(0, 11);

      const body = Buffer.alloc(2 + 1 + apiKeys.length + 4 + 1);
      let offset = 0;

      body.writeInt16BE(0, offset);
      offset += 2;
      body[offset] = 3;
      offset += 1;
      apiKeys.copy(body, offset);
      offset += apiKeys.length;
      body.writeInt32BE(0, offset);
      offset += 4;
      body[offset] = 0;

      const response = Buffer.alloc(4 + 4 + body.length);
      response.writeUInt32BE(4 + body.length, 0);
      response.writeInt32BE(correlationId, 4);
      body.copy(response, 8);

      connection.write(response);
    }
  });
});

server.listen(9092, "127.0.0.1");
