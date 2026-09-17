import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  let buffer = Buffer.alloc(0);

  connection.on("data", (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);

    if (buffer.length < 12) {
      return;
    }

    const correlationId = buffer.readInt32BE(8);
    const response = Buffer.alloc(8);
    response.writeUInt32BE(0, 0);
    response.writeInt32BE(correlationId, 4);

    connection.end(response);
  });
});

server.listen(9092, "127.0.0.1");
