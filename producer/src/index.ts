import express, { Application, Request, Response } from 'express';
import http from 'http';
import * as dotenv from 'dotenv';
import cors from 'cors';
import queue, { Queue, emitter } from './ampq';

dotenv.config();

class Server {
  private app: Application;
  private server: http.Server;

  constructor() {
    this.app = express();
    this.config();
    this.server = http.createServer(this.app);
    this.routes();
  }

  async connect(queue: Queue) {
    await queue.connect();
    await queue.produce();
    await queue.consume();
  }

  private routes(): void {
    this.app.get('*', (req: Request, res: Response) => {
      res.status(404).send('NOT FOUND');
    });
    this.app.post('/', (req: Request, res: Response) => {
      res.writeHead(200, {
        Connection: 'keep-alive',
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      });
      let data = req.body;
      emitter.emit('produce', data);
      emitter.on('response', (msg) => {
        res.write(msg);
        res.end();
      });
    });
  }

  private config(): void {
    this.app.set('port', process.env.M1 || 3000);
    this.app.use(
      cors({
        credentials: true,
      })
    );
    this.app.use(express.json());
  }

  public start(): void {
    this.server.listen(this.app.get('port'), () => {
      console.log(`Producer is running at ${this.app.get('port')}`);
    });
  }
}

const server = new Server();

server.connect(queue);

server.start();
