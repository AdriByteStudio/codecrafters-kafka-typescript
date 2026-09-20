import net from "net";
import { readFileSync } from "fs";

type PartitionMetadata = {
  partitionId: number;
  topicId: Buffer;
  leaderId: number;
  leaderEpoch: number;
  replicas: number[];
  isr: number[];
};

type TopicMetadata = {
  topicId: Buffer;
  partitions: PartitionMetadata[];
};

const metadataLogPath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

function readVarint(buffer: Buffer, offset: number): [number, number] {
  let value = 0;
  let shift = 0;

  while (offset < buffer.length) {
    const byte = buffer[offset++];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      return [(value >>> 1) ^ -(value & 1), offset];
    }
    shift += 7;
  }

  throw new Error("truncated varint");
}

function readUnsignedVarint(buffer: Buffer, offset: number): [number, number] {
  let value = 0;
  let shift = 0;

  while (offset < buffer.length) {
    const byte = buffer[offset++];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      return [value, offset];
    }
    shift += 7;
  }

  throw new Error("truncated unsigned varint");
}

function readCompactIntArray(buffer: Buffer, offset: number): [number[], number] {
  const [encodedLength, nextOffset] = readUnsignedVarint(buffer, offset);
  const values: number[] = [];
  let currentOffset = nextOffset;

  for (let index = 0; index < encodedLength - 1; index += 1) {
    values.push(buffer.readInt32BE(currentOffset));
    currentOffset += 4;
  }

  return [values, currentOffset];
}

function readTopicRecord(value: Buffer, messageOffset: number): { name: string; topicId: Buffer } {
  let offset = messageOffset;
  let nameLength: number;

  if (value[offset] === 0) {
    nameLength = value.readUInt16BE(offset);
    offset += 2;
  } else {
    const [encodedLength, nameOffset] = readUnsignedVarint(value, offset);
    nameLength = encodedLength - 1;
    offset = nameOffset;
  }

  const name = value.subarray(offset, offset + nameLength).toString();
  offset += nameLength;

  return { name, topicId: Buffer.from(value.subarray(offset, offset + 16)) };
}

function readPartitionRecord(value: Buffer, messageOffset: number): PartitionMetadata {
  let offset = messageOffset;
  const partitionId = value.readInt32BE(offset);
  offset += 4;
  const topicId = Buffer.from(value.subarray(offset, offset + 16));
  offset += 16;

  const [replicas, replicasOffset] = readCompactIntArray(value, offset);
  const [isr, isrOffset] = readCompactIntArray(value, replicasOffset);
  const [, removingReplicasOffset] = readCompactIntArray(value, isrOffset);
  const [, addingReplicasOffset] = readCompactIntArray(value, removingReplicasOffset);
  offset = addingReplicasOffset;

  const leaderId = value.readInt32BE(offset);
  offset += 4;
  const leaderEpochOffset = offset + 1;
  const leaderEpoch = value.readInt32BE(leaderEpochOffset);

  return { partitionId, topicId, leaderId, leaderEpoch, replicas, isr };
}

function readMetadataLog(): Map<string, TopicMetadata> {
  const topics = new Map<string, TopicMetadata>();
  let log: Buffer;

  try {
    log = readFileSync(metadataLogPath);
  } catch {
    return topics;
  }

  let offset = 0;
  const partitionsByTopic = new Map<string, PartitionMetadata[]>();

  while (offset + 61 <= log.length) {
    const batchLength = log.readInt32BE(offset + 8);
    const batchEnd = offset + 12 + batchLength;
    const recordsCount = log.readInt32BE(offset + 57);
    let recordOffset = offset + 61;

    if (batchLength <= 0 || batchEnd > log.length || recordsCount < 0) {
      break;
    }

    for (let recordIndex = 0; recordIndex < recordsCount && recordOffset < batchEnd; recordIndex += 1) {
      const [recordLength, recordStart] = readVarint(log, recordOffset);
      const recordEnd = recordStart + recordLength;

      if (recordLength <= 0 || recordEnd > batchEnd || recordEnd <= recordOffset) {
        break;
      }

      let currentOffset = recordStart + 1;
      [, currentOffset] = readVarint(log, currentOffset);
      [, currentOffset] = readVarint(log, currentOffset);

      const [keyLength, keyOffset] = readVarint(log, currentOffset);
      currentOffset = keyOffset + Math.max(keyLength, 0);
      const [valueLength, recordValueOffset] = readVarint(log, currentOffset);
      const value = log.subarray(recordValueOffset, recordValueOffset + valueLength);
      recordOffset = recordEnd;

      try {
        if (value.length < 3) {
          continue;
        }

        let valueOffset = 0;
        [, valueOffset] = readUnsignedVarint(value, valueOffset);
        const [apiKey, apiKeyOffset] = readUnsignedVarint(value, valueOffset);
        const [, messageOffset] = readUnsignedVarint(value, apiKeyOffset);
        if (apiKey === 2) {
          const record = readTopicRecord(value, messageOffset);
          topics.set(record.name, {
            topicId: record.topicId,
            partitions: partitionsByTopic.get(record.topicId.toString("hex")) ?? [],
          });
        } else if (apiKey === 3) {
          const partition = readPartitionRecord(value, messageOffset);
          const topicPartitions = partitionsByTopic.get(partition.topicId.toString("hex")) ?? [];
          topicPartitions.push(partition);
          partitionsByTopic.set(partition.topicId.toString("hex"), topicPartitions);
        }
      } catch {
        continue;
      }
    }

    offset = batchEnd;
  }

  for (const topic of topics.values()) {
    topic.partitions = partitionsByTopic.get(topic.topicId.toString("hex")) ?? [];
  }

  return topics;
}

function parseDescribeRequest(request: Buffer, clientIdLength: number): { topicNames: string[]; end: number } {
  let offset = 14 + Math.max(clientIdLength, 0) + 1;
  const [topicCount, topicsOffset] = readUnsignedVarint(request, offset);
  offset = topicsOffset;
  const topicNames: string[] = [];

  for (let index = 0; index < topicCount - 1; index += 1) {
    const [nameLength, nameOffset] = readUnsignedVarint(request, offset);
    offset = nameOffset;
    topicNames.push(request.subarray(offset, offset + nameLength - 1).toString());
    offset += nameLength - 1;
    offset += 1;
  }

  offset += 4 + 1 + 1;
  return { topicNames, end: offset };
}

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
        try {
          const request = parseDescribeRequest(buffer, clientIdLength);
          messageLength = Math.max(messageLength, request.end);
        } catch {
          return;
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

      if (apiKey === 1) {
        const body = Buffer.alloc(4 + 2 + 4 + 1 + 1);
        let offset = 0;
        body.writeInt32BE(0, offset);
        offset += 4;
        body.writeInt16BE(0, offset);
        offset += 2;
        body.writeInt32BE(0, offset);
        offset += 4;
        body[offset++] = 1;
        body[offset] = 0;

        const response = Buffer.alloc(4 + 4 + 1 + body.length);
        response.writeUInt32BE(4 + 1 + body.length, 0);
        response.writeInt32BE(correlationId, 4);
        response[8] = 0;
        body.copy(response, 9);
        connection.write(response);
        continue;
      }

      if (apiKey === 18 && (version < 0 || version > 4)) {
        const response = Buffer.alloc(10);
        response.writeUInt32BE(6, 0);
        response.writeInt32BE(correlationId, 4);
        response.writeInt16BE(35, 8);
        connection.write(response);
        continue;
      }

      if (apiKey === 75) {
        const clientIdLength = request.readInt16BE(12);
        const requestTopics = parseDescribeRequest(request, clientIdLength).topicNames;
        const metadataByName = readMetadataLog();
        const topicResponses = requestTopics.sort((left, right) => left.localeCompare(right)).map((topicName) => {
          const topicNameBuffer = Buffer.from(topicName);
          const metadata = metadataByName.get(topicName);
          const partitions = metadata?.partitions ?? [];
          const partitionResponses = partitions.map((partition) => {
            const response = Buffer.alloc(
              2 + 4 + 4 + 4 + 1 + partition.replicas.length * 4 + 1 + partition.isr.length * 4 + 1 + 1 + 1 + 1,
            );
            let offset = 0;
            response.writeInt16BE(0, offset);
            offset += 2;
            response.writeInt32BE(partition.partitionId, offset);
            offset += 4;
            response.writeInt32BE(partition.leaderId, offset);
            offset += 4;
            response.writeInt32BE(partition.leaderEpoch, offset);
            offset += 4;
            response[offset++] = partition.replicas.length + 1;
            for (const replica of partition.replicas) {
              response.writeInt32BE(replica, offset);
              offset += 4;
            }
            response[offset++] = partition.isr.length + 1;
            for (const replica of partition.isr) {
              response.writeInt32BE(replica, offset);
              offset += 4;
            }
            response[offset++] = 1;
            response[offset++] = 1;
            response[offset++] = 1;
            response[offset] = 0;
            return response;
          });

          return Buffer.concat([
            Buffer.from([metadata ? 0 : 0, metadata ? 0 : 3]),
            Buffer.from([topicNameBuffer.length + 1]),
            topicNameBuffer,
            metadata?.topicId ?? Buffer.alloc(16),
            Buffer.from([0, partitions.length + 1]),
            ...partitionResponses,
            Buffer.from([0, 0, 0, 0, 0]),
          ]);
        });

        const body = Buffer.concat([
          Buffer.from([0, 0, 0, 0, topicResponses.length + 1]),
          ...topicResponses,
          Buffer.from([0xff, 0]),
        ]);

        const response = Buffer.alloc(4 + 4 + 1 + body.length);
        response.writeUInt32BE(4 + 1 + body.length, 0);
        response.writeInt32BE(correlationId, 4);
        response[8] = 0;
        body.copy(response, 9);

        connection.write(response);
        continue;
      }

      const apiKeys = Buffer.alloc(21);
      apiKeys.writeInt16BE(1, 0);
      apiKeys.writeInt16BE(0, 2);
      apiKeys.writeInt16BE(16, 4);
      apiKeys.writeInt16BE(18, 7);
      apiKeys.writeInt16BE(0, 9);
      apiKeys.writeInt16BE(4, 11);
      apiKeys.writeInt16BE(75, 14);
      apiKeys.writeInt16BE(0, 16);
      apiKeys.writeInt16BE(0, 18);

      const body = Buffer.alloc(2 + 1 + apiKeys.length + 4 + 1);
      let offset = 0;

      body.writeInt16BE(0, offset);
      offset += 2;
      body[offset] = 4;
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
