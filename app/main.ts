import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  let buffer = Buffer.alloc(0);

  connection.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 4) {
      const messageSize = buffer.readUInt32BE(0);
      let messageLength = 4 + messageSize;
      const frameApiKey = buffer.length >= 6 ? buffer.readInt16BE(4) : 0;

      if (frameApiKey === 75 && buffer.length >= 14) {
        const clientIdLength = buffer.readInt16BE(12);
        const topicsOffset = 14 + Math.max(clientIdLength, 0) + 1;

        if (buffer.length > topicsOffset + 1) {
          const topicNameLength = buffer[topicsOffset + 1] - 1;
          const requestEnd = topicsOffset + 1 + 1 + topicNameLength + 1 + 4 + 1 + 1;
          messageLength = Math.max(messageLength, requestEnd);
        }
      } else if (buffer.length >= 14) {
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
      const apiKey = request.readInt16BE(4);

      if (version < 0 || version > 4) {
        const response = Buffer.alloc(10);
        response.writeUInt32BE(6, 0);
        response.writeInt32BE(correlationId, 4);
        response.writeInt16BE(35, 8);
        connection.write(response);
        continue;
      }

      if (apiKey === 75) {
        const clientIdLength = request.readInt16BE(12);
        const topicsOffset = 14 + Math.max(clientIdLength, 0) + 1;
        const topicNameLength = request[topicsOffset + 1] - 1;
        const topicName = request.subarray(topicsOffset + 2, topicsOffset + 2 + topicNameLength);

        const topic = Buffer.alloc(2 + 1 + topicName.length + 16 + 1 + 1 + 4 + 1);
        let topicOffset = 0;
        topic.writeInt16BE(3, topicOffset);
        topicOffset += 2;
        topic[topicOffset] = topicName.length + 1;
        topicOffset += 1;
        topicName.copy(topic, topicOffset);
        topicOffset += topicName.length;
        topicOffset += 16;
        topic[topicOffset] = 0;
        topicOffset += 1;
        topic[topicOffset] = 1;
        topicOffset += 1;
        topic.writeInt32BE(0, topicOffset);
        topicOffset += 4;
        topic[topicOffset] = 0;

        const body = Buffer.alloc(4 + 1 + topic.length + 1 + 1);
        let bodyOffset = 0;
        body.writeInt32BE(0, bodyOffset);
        bodyOffset += 4;
        body[bodyOffset] = 2;
        bodyOffset += 1;
        topic.copy(body, bodyOffset);
        bodyOffset += topic.length;
        body[bodyOffset] = 0xff;
        bodyOffset += 1;
        body[bodyOffset] = 0;

        const response = Buffer.alloc(4 + 4 + 1 + body.length);
        response.writeUInt32BE(4 + 1 + body.length, 0);
        response.writeInt32BE(correlationId, 4);
        response[8] = 0;
        body.copy(response, 9);

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
