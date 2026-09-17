import net from "net";

const server: net.Server = net.createServer((connection: net.Socket) => {
  const response = Buffer.alloc(8);
  response.writeUInt32BE(0, 0);
  response.writeUInt32BE(7, 4);

  connection.end(response);
});

server.listen(9092, "127.0.0.1");
