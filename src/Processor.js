
import _ from 'lodash';

export default class Processor {
  processorFunc;
  running = false;

  constructor(processorFunc) {
    this.processorFunc = processorFunc;
  }

  start(task, done) {
    this.lock();
    // TODO: `.touch` method for reset `Lock`

    let timeout = setTimeout(() => {
      this.unlock();
      done(new Error(`timeout ${task.timeout / 1000}sec reached.`));
    }, task.timeout);

    this.processorFunc(_.cloneDeep(task), (err) => {
      this.unlock();
      clearTimeout(timeout);
      done(err);
    });
  }

  lock() {
    this.running = true;
  }

  unlock() {
    this.running = false;
  }
}

