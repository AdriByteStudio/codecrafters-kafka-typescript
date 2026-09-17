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
    const errorCode = version >= 0 && version <= 4 ? 0 : 35;

    const response = Buffer.alloc(10);
    response.writeUInt32BE(0, 0);
    response.writeInt32BE(correlationId, 4);
    response.writeInt16BE(errorCode, 8);

    connection.end(response);
  });
});

server.listen(9092, "127.0.0.1");
