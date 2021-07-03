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
  const jobsToRecord = [];
  let addJobScheduled = false;

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

  function pushJob(job) {
    jobsToRecord.push(job);

    if (!addJobScheduled) {
      process.nextTick(recordJobs);
      addJobScheduled = true;
    }
  }

  async function recordJobs() {
    try {
      addJobScheduled = false;
      if (jobsToRecord.length === 0) {
        return;
      }

      await collection.insertMany(jobsToRecord.splice(0, jobsToRecord.length));
      await updateTimeout();
    } catch (error) {
      handleError(error);
    }
  }

  return {
    async start() {
      await collection.createIndex({date: 1});
      await updateTimeout();
    },

    define(name, callback) {
      fns[name] = callback;
    },

    addJob: job => pushJob(checkJobDate(job)),
    addJobs: jobs => jobs.map(checkJobDate).forEach(pushJob),

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
