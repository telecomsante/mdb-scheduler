module.exports = ({collection, handleError = console.error}) => {
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

        (async () => fn(data))().catch(handleError);
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

    async addJob({date, name, data}) {
      date = new Date(date);
      if (Number.isNaN(date.getTime())) {
        throw new TypeError('Invalid date');
      }

      await collection.insertOne({date, name, data});
      await updateTimeout();
    },

    async delJob(search) {
      const {deletedCount} = await collection.deleteMany(search);
      await updateTimeout();
      return deletedCount;
    },
  };
};
