const test = require('ava');

const createTimeMock = require('../mock/time');
const createMongoMock = require('../mock/mongodb');
const createHandleErrorMock = require('../mock/handle-error.js');

const createScheduler = require('../../index.js');

test.beforeEach(t => {
  const {setTimeout, clearTimeout, Date, tasksDelay, executeNextTask} = createTimeMock();
  const db = createMongoMock();
  const handleErrorMock = createHandleErrorMock();

  const scheduler = createScheduler({
    collection: db.collection(),
    handleError: handleErrorMock,
    setTimeout,
    clearTimeout,
    Date,
  });

  t.context = {
    scheduler,
    tasksDelay,
    executeNextTask,
    db,
    handleErrorMock,
    Date,
  };
});

const nextTick = () => new Promise(resolve => {
  process.nextTick(resolve);
});

const dateIn = (Date, time) => new Date(Date.now() + time);

const checkNextJob = async (t, date) => {
  const {db} = t.context;

  await nextTick();
  t.deepEqual(
    db.getRequests(),
    [{type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]}],
  );
  db.respondRequest(0, date ? {date} : null);
};

const testStart = async t => {
  const {scheduler, tasksDelay, db, handleErrorMock} = t.context;

  let finished = false;
  const addJobPromise = scheduler.start()
    .then(() => finished || t.fail('addJob returned before finish'))
    .catch(error => t.fail(`addJob failed with error: ${error}`));

  t.deepEqual(db.getRequests(), [{type: 'createIndex', index: {date: 1}}]);
  db.respondRequest(0);

  checkNextJob(t, null);
  finished = true;

  await addJobPromise;

  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
};

const scheduleJob = async ({t, name, date, data, nextDate}) => {
  const {scheduler, db} = t.context;

  let finished = false;
  const addJobPromise = scheduler.addJob({name, date, data})
    .then(() => finished || t.fail('addJob returned before finish'))
    .catch(error => t.fail(`addJob failed with error: ${error}`));

  t.deepEqual(db.getRequests(), [{type: 'insertOne', element: {name, date, data}}]);
  db.respondRequest(0, {});

  await checkNextJob(t, nextDate || date);
  finished = true;

  await addJobPromise;
};

const cancelJob = async ({t, search, nextDate}) => {
  const {scheduler, db} = t.context;

  let finished = false;
  const delJobPromise = scheduler.delJob(search)
    .then(result => {
      if (!finished) {
        t.fail('addJob returned before finish');
      }

      return result;
    })
    .catch(error => t.fail(`addJob failed with error: ${error}`));

  t.deepEqual(db.getRequests(), [{type: 'deleteMany', search}]);
  db.respondRequest(0, {deletedCount: 1});

  await checkNextJob(t, nextDate || null);
  finished = true;

  const deletedCount = await delJobPromise;
  t.is(deletedCount, 1);
};

const executeJob = async ({t, name, data}) => {
  const {executeNextTask, db, Date} = t.context;

  executeNextTask().catch(t.fail);

  t.deepEqual(
    db.getRequests(),
    [{type: 'findOneAndDelete', search: {date: new Date()}}],
  );
  db.respondRequest(0, {value: {name, date: new Date(), data}});

  await nextTick();
};

test('/scheduler/schedule-one-job', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data'});
  t.deepEqual(dataReceived, ['data']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await checkNextJob(t, null);
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/add-another-job-after', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await scheduleJob({t, name: 'test', date: dateIn(Date, 2000), nextDate: dateIn(Date, 1000), data: 'data2'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data1'});
  t.deepEqual(dataReceived, ['data1']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await checkNextJob(t, dateIn(Date, 1000));
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data1']);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data2'});
  t.deepEqual(dataReceived, ['data1', 'data2']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await checkNextJob(t, null);
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data1', 'data2']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/add-another-job-before', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await scheduleJob({t, name: 'test', date: dateIn(Date, 500), data: 'data2'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [500]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data2'});
  t.deepEqual(dataReceived, ['data2']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await checkNextJob(t, dateIn(Date, 500));
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data2']);
  t.deepEqual(tasksDelay(), [500]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data1'});
  t.deepEqual(dataReceived, ['data2', 'data1']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await checkNextJob(t, null);
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data2', 'data1']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/remove-only-job', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await cancelJob({t, search: {name: 'test'}, nextDate: null});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/remove-next-job', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await scheduleJob({t, name: 'test', date: dateIn(Date, 2000), nextDate: dateIn(Date, 1000), data: 'data2'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await cancelJob({t, search: {data: 'data1'}, nextDate: dateIn(Date, 2000)});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [2000]);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/remove-non-next-job', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await scheduleJob({t, name: 'test', date: dateIn(Date, 2000), nextDate: dateIn(Date, 1000), data: 'data2'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await cancelJob({t, search: {data: 'data2'}, nextDate: dateIn(Date, 1000)});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/no-job-found-in-delJob', async t => {
  const {db, scheduler} = t.context;

  await testStart(t);

  let finished = false;
  const delJobPromise = scheduler.delJob({})
    .then(result => {
      if (!finished) {
        t.fail('addJob returned before finish');
      }

      return result;
    })
    .catch(error => t.fail(`addJob failed with error: ${error}`));

  t.deepEqual(db.getRequests(), [{type: 'deleteMany', search: {}}]);
  finished = true;
  db.respondRequest(0, {deletedCount: 0});

  const deletedCount = await delJobPromise;
  t.is(deletedCount, 0);
});

test('/scheduler/do-not-execute-removed-task', async t => {
  const {scheduler, db, tasksDelay, executeNextTask, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data1'});
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  let finished = false;
  const delJobPromise = scheduler.delJob({name: 'test'})
    .then(result => {
      if (!finished) {
        t.fail('addJob returned before finish');
      }

      return result;
    })
    .catch(error => t.fail(`addJob failed with error: ${error}`));

  t.deepEqual(db.getRequests(), [{type: 'deleteMany', search: {name: 'test'}}]);
  db.respondRequest(0, {deletedCount: 1});

  await nextTick();
  t.deepEqual(
    db.getRequests(),
    [{type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]}],
  );

  executeNextTask().catch(t.fail);
  t.deepEqual(db.getRequests(), [
    {type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]},
    {type: 'findOneAndDelete', search: {date: new Date()}},
  ]);

  db.respondRequest(1, {value: null});
  t.deepEqual(db.getRequests(), [
    {type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]},
  ]);

  finished = true;
  db.respondRequest(0, null);

  const deletedCount = await delJobPromise;
  t.is(deletedCount, 1);

  t.deepEqual(db.getRequests(), [
    {type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]},
  ]);
  db.respondRequest(0, null);

  await nextTick();
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);
});

test('/scheduler/unkown-job-name', async t => {
  const {db, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data'});
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), [new Error('Unknown job test')]);

  await checkNextJob(t, null);
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), [new Error('Unknown job test')]);
});

test('/scheduler/invalid-date', async t => {
  const {scheduler} = t.context;

  await testStart(t);

  try {
    await scheduler.addJob({name: 'test', date: 'invalid', data: 'date'});
    t.fail('addJob must throw an error');
  } catch {
    t.pass();
  }
});

test('/scheduler/without-time-mock', async t => {
  const db = createMongoMock();
  const handleErrorMock = createHandleErrorMock();
  const scheduler = createScheduler({
    collection: db.collection(),
    handleError: handleErrorMock,
  });

  t.context.scheduler = scheduler;
  t.context.db = db;

  await testStart(t);

  let callbackCalled = false;
  scheduler.define('test', () => {
    callbackCalled = true;
    throw new Error('test error');
  });

  const date = new Date();
  await scheduleJob({t, name: 'test', date, data: 'data'});

  await new Promise(resolve => {
    setTimeout(resolve, 1);
  });
  t.deepEqual(
    db.getRequests(),
    [{type: 'findOneAndDelete', search: {date}}],
  );
  t.deepEqual(handleErrorMock.getErrors(), []);
  db.respondRequest(0, {value: {name: 'test', date, data: 'data'}});

  await checkNextJob(t, null);
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(handleErrorMock.getErrors(), [new Error('test error')]);
  t.is(callbackCalled, true);
});

test('/scheduler/error-on-search-next-date', async t => {
  const {db, scheduler, tasksDelay, handleErrorMock, Date} = t.context;

  await testStart(t);

  const dataReceived = [];
  scheduler.define('test', data => dataReceived.push(data));

  await scheduleJob({t, name: 'test', date: dateIn(Date, 1000), data: 'data'});
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, []);
  t.deepEqual(tasksDelay(), [1000]);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await executeJob({t, name: 'test', data: 'data'});
  t.deepEqual(dataReceived, ['data']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), []);

  await nextTick();
  t.deepEqual(
    db.getRequests(),
    [{type: 'aggregate', pipeline: [{$group: {_id: {}, date: {$min: '$date'}}}]}],
  );
  db.respondRequest(0, null, new Error('aggregate error'));

  await nextTick();
  t.deepEqual(db.getRequests(), []);
  t.deepEqual(dataReceived, ['data']);
  t.deepEqual(tasksDelay(), []);
  t.deepEqual(handleErrorMock.getErrors(), [new Error('aggregate error')]);
});
