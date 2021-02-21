const {randomUUID} = require('crypto');

module.exports = () => {
  let tasks = [];
  let time = Date.now();

  function DateMock(inputDate) {
    return new Date(inputDate || time);
  }

  DateMock.now = () => time;

  return {
    setTimeout(callback, delay) {
      const newTask = {
        timeoutID: randomUUID(),
        delay: Math.max(0, delay),
        callback,
      };
      tasks.push(newTask);
      tasks.sort((a, b) => a.delay > b.delay ? 1 : (a.delay < b.delay ? -1 : 0));
      return newTask.timeoutID;
    },
    clearTimeout(timeoutID) {
      tasks = tasks.filter(i => i.timeoutID !== timeoutID);
    },
    Date: DateMock,

    tasksDelay: () => tasks.map(i => i.delay),
    async executeNextTask() {
      if (tasks.length === 0) {
        throw new Error('No task to execute');
      }

      const {delay, callback} = tasks.shift();
      time += delay;
      tasks = tasks.map(i => ({...i, delay: i.delay - delay}));

      await callback();
    },
  };
};
