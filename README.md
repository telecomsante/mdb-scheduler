Mongodb backed nodejs scheduler

# Install

```bash
npm install mdb-scheduler
```

# Example

## Task scheduling web server

Setup a web server that schedule task

If the server is down when the task should be executed, the task is executed at the next start

```js
import mongodb from 'mongodb';
import Fastify from 'fastify';
import dayjs from 'dayjs';
import bunyan from 'bunyan';
import mdbScheduler from 'mdb-scheduler';

const log = bunyan.createLogger({name: 'myapp'});

// Connect to database

const client = await mongodb.MongoClient.connect('mongodb://localhost');
const db = client.db('mydb');

// Create scheduler

const scheduler = mdbScheduler({
  collection: db.collection('jobs'),
  handleError: error => log.error(error),
});

scheduler.define('job', async (data, date) => {
  try {
    log.info(`Execute scheduled task for user ${data.userID} (planned for ${date})`);
    // Do some stuff
  } catch (error) {
    log.error(error);
  }
});

await scheduler.start();

// Create web server

const fastify = Fastify();

fastify.post('/users/:userID/schedule', async (request, reply) => {
  await scheduler.addJob({
    name: 'job',
    data: {userID: request.params.userID},
    date: dayjs().add(1, 'day'),
  });

  reply.send();
});

fastify.post('/users/:userID/cancel', async (request, reply) => {
  const deletedCount = await scheduler.delJob({
    name: 'job',
    'data.userID': request.params.userID,
  });

  if (deletedCount === 0) {
    reply.code(404);
    reply.send('Task not found');
  } else {
    reply.send();
  }
});

fastify.listen(8080, error => {
  if (error) {
   log.error(error);
  }

  log.info('listen port 8080');
});
```

## Recurrent Task

Execute a task each day at midnight

If the server is down at midnight, the task is executed at the next start

The function nextDate should return the date of the next execution

```js
import mongodb from 'mongodb';
import dayjs from 'dayjs';
import bunyan from 'bunyan';
import mdbScheduler from 'mdb-scheduler';

const log = bunyan.createLogger({name: 'myapp'});

// Connect to database

const client = await mongodb.MongoClient.connect('mongodb://localhost');
const db = client.db('mydb');

// Create scheduler

const scheduler = mdbScheduler({
  collection: db.collection('jobs'),
  handleError: error => log.error(error),
});

scheduler.define('job', async (_, date) => {
  try {
    log.info(`Execute recurrent task planned for ${date}`);
    // Do some stuff
  } catch (error) {
    log.error(error);
  }
});

scheduler.every({
  name: 'job',
  nextDate: () => dayjs().endOf('day'),
});

await scheduler.start();
```

If you want to use the cron syntax you can use [node-cron](https://github.com/kelektiv/node-cron). Example:

```js
import {CronJob} from 'cron'

const cron = new CronJob('0 0 * * *');
scheduler.every({
  name: 'job',
  nextDate: () => cron.nextDate(),
});
```
