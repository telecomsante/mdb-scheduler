
const flat = o => Object.fromEntries(Object.entries(o)
  .flatMap(([key, value]) =>
    (value && value.constructor === Object ? Object.entries(value) : [['', value]])
      .map(([subKey, subValue]) => [`${key}${subKey ? '.' : ''}${subKey}`, subValue])
  ));

module.exports = ({collection, log = () => {}}) => {
  let timeoutID;
  const fns = {};

  async function updateTimeout() {
    const dateDoc = await collection.aggregate([{
      $group: {_id: {}, date: {$min: '$date'}}
    }]).next();
    if (!dateDoc) {
      return;
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

        (async () => fn(data))().catch(log);
      } catch (error) {
        log(error);
      } finally {
        updateTimeout().catch(log);
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

    async delJob({name, dataMatch = {}}) {
      const {deletedCount} = await collection.deleteMany(flat({data: dataMatch, name}));
      await updateTimeout();
      return deletedCount;
    }
  };
};
