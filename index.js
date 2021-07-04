function checkJobDate(job) {
  const date = new Date(job.date);
  if (Number.isNaN(date.getTime())) {
    throw new TypeError('Invalid date');
  }

  return {...job, date};
}

module.exports = ({
  collection,
  handleError = console.error,
  setTimeout = global.setTimeout,
  clearTimeout = global.clearTimeout,
  Date = global.Date,
}) => {
  let timeoutID;
  const fns = {};

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

        const {name, data} = job;

        const fn = fns[name];
        if (!fn) {
          throw new Error(`Unknown job ${name}`);
        }

        (async () => fn(data, date))().catch(handleError);
      } catch (error) {
        handleError(error);
      } finally {
        updateTimeout().catch(handleError);
      }
    }, date - Date.now());
  }

  return {
    async start() {
      await collection.createIndex({date: 1});
      await updateTimeout();
    },

    define(name, callback) {
      fns[name] = callback;
    },

    async addJob(job) {
      await collection.insertOne(checkJobDate(job));
      await updateTimeout();
    },

    async addJobs(jobs) {
      await collection.insertMany(jobs.map(checkJobDate));
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
  };
};
