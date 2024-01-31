import amqp, { Connection } from "amqplib";
import { request_queue, response_queue } from "./config";
import * as dotenv from "dotenv";
import { EventEmitter } from "stream";
import { Logger } from "./utils";

dotenv.config();

export const emitter = new EventEmitter();

export interface Queue {
  connect(): Promise<void>;
  consume(): Promise<void>;
  produce(): Promise<void>;
}

class RabbitMQ implements Queue {
  private ampqUrl: string;
  public connection: Connection | undefined;

  constructor(ampqUrl: string | undefined) {
    if (typeof ampqUrl === "undefined") {
      throw Error("RABBITMQ_URL is not set in .env");
    }
    this.ampqUrl = ampqUrl;
  }

  async connect() {
    try {
      this.connection = await amqp.connect(this.ampqUrl);
      Logger(`Connected to RabbitMQ`);
    } catch (error: any) {
      Logger(`RABBITMQ_CONNECTION_ERROR: ${error.message}`);
    }
  }

  async produce() {
    if (!this.connection) {
      throw new Error("RABBITMQ_CONNECTION_ERROR: Connection not established");
    }
    const produce_channel = await this.connection.createChannel();
    produce_channel.assertQueue(request_queue, { durable: true });
    Logger(`PRODUCE CHANNEL CREATED`);
    emitter.on("produce", (msg) => {
      const message = JSON.stringify(msg);
      produce_channel.sendToQueue(request_queue, Buffer.from(message));
      Logger(`Produced Message: ${message}`);
    });
  }

  async consume() {
    if (!this.connection) {
      throw new Error("RABBITMQ_CONNECTION_ERROR: Connection not established");
    }
    const consume_channel = await this.connection.createChannel();
    consume_channel.assertQueue(response_queue, { durable: true });
    Logger(`CONSUME CHANNEL CREATED`);
    await consume_channel.consume(response_queue, (data) => {
      if (!data) {
        emitter.emit("consume", "RABBITMQ_DATA: No Data");
        return Logger(`RABBITMQ_DATA: No Data`);
      }
      consume_channel.ack(data);
      const msg = `Acknowledged Message: ${JSON.stringify(data.content.toString())}`;
      emitter.emit("response", msg);
      Logger(msg);
    });
  }
}

const queue: RabbitMQ = new RabbitMQ(process.env.RABBITMQ_URL);

export default queue;
