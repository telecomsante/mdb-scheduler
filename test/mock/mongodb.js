module.exports = () => {
  const requests = [];

  function addRequest(cmd) {
    let resolvePromise;
    let rejectPromise;
    const promise = new Promise((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    requests.push({resolvePromise, rejectPromise, cmd});
    return promise;
  }

  return {
    collection: () => ({
      createIndex: index => addRequest({type: 'createIndex', index}),
      insertOne: element => addRequest({type: 'insertOne', element}),
      deleteMany: search => addRequest({type: 'deleteMany', search}),
      findOneAndDelete: search => addRequest({type: 'findOneAndDelete', search}),
      aggregate: pipeline => ({next: () => addRequest({type: 'aggregate', pipeline})}),
    }),

    getRequests: () => requests.map(i => i.cmd),
    respondRequest(index, response, error) {
      const request = requests.splice(index, 1)[0];
      if (!request) {
        throw new Error(`Request ${index} not found`);
      }

      request[error ? 'rejectPromise' : 'resolvePromise'](error || response);
    },
  };
};
