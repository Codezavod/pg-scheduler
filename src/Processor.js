
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
    this.processorFunc(_.cloneDeep(task), (err) => {
      this.unlock();
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

