function parseDate(date) {
  date = new Date(date);
  if (Number.isNaN(date.getTime())) {
    throw new TypeError('Invalid date');
  }

  return date;
}

module.exports = ({
  collection,
  handleError = console.error,
  setTimeout = global.setTimeout,
  clearTimeout = global.clearTimeout,
  Date = global.Date,
}) => {
  let timeoutID;
  const fns = {jobs: {}, recurrent: {}};

  const getJob = name => {
    const fn = fns.jobs[name];
    if (!fn) {
      throw new Error(`Unknown job "${name}"`);
    }

    return fn;
  };

  async function updateTimeout() {
    const dateDoc = await collection.aggregate([{
      $group: {_id: {}, date: {$min: '$date'}},
    }]).next();
    if (!dateDoc) {
      return clearTimeout(timeoutID);
    }

    const {date} = dateDoc;

    clearTimeout(timeoutID);
    timeoutID = setTimeout(async () => {
      try {
        const {value: job} = await collection.findOneAndDelete({date});
        if (!job) {
          return;
        }

        const {name, data, recurrent} = job;
        const fn = recurrent ? (fns.recurrent[name] || (() => {})) : getJob(name);

        (async () => fn(data, date))().catch(handleError);
      } catch (error) {
        handleError(error);
      } finally {
        updateTimeout().catch(handleError);
      }
    }, date - Date.now());
  }

  const scheduler = {
    async start() {
      await collection.createIndex({date: 1});
      await updateTimeout();
    },

    define(name, callback) {
      fns.jobs[name] = callback;
    },

    async addJob({date, name, data}) {
      await collection.insertOne({date: parseDate(date), name, data});
      await updateTimeout();
    },

    async delJob(search) {
      const {deletedCount} = await collection.deleteMany(search);
      if (deletedCount > 0) {
        await updateTimeout();
      }

      return deletedCount;
    },

    findJob: search => collection.find(search).toArray(),

    every({nextDate, name}) {
      fns.recurrent[name] = async (_, date) => {
        await (async () => getJob(name)(null, date))().catch(handleError);
        scheduler.every({nextDate, name});
      };

      const date = parseDate(nextDate());
      if (date < Date.now()) {
        throw Object.assign(
          new Error('Do not schedule recurrent task in the past'),
          {date, job: name},
        );
      }

      (async () => {
        const {upsertedId} = await collection.updateOne(
          {name, recurrent: true},
          {$setOnInsert: {date, name, recurrent: true}},
          {upsert: true},
        );
        if (upsertedId) {
          await updateTimeout();
        }
      })().catch(handleError);
    },
  };

  return scheduler;
};
