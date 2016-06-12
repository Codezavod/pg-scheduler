
import _ from 'lodash';

export default class Processor {
  processorFunc;
  runningCount = 0;
  options = {
    concurrency: 1
  };

  constructor(processorFunc, options) {
    this.processorFunc = processorFunc;
    this.options = options;
  }

  start(task, done) {
    this.lock();

    let timeoutHandler = () => {
          console.error(`processor end with timeout (${task.timeout / 1000}sec) for task`, task.toJSON());
          this.unlock();
          done(new Error(`timeout ${task.timeout / 1000}sec reached.`));
        },
        timeout = setTimeout(timeoutHandler, task.timeout),
        touchHandler = () => {
          clearTimeout(timeout);
          timeout = setTimeout(timeoutHandler, task.timeout);
        };

    task.on('touch', touchHandler);

    this.processorFunc(_.cloneDeep(task), (err) => {
      this.unlock();
      clearTimeout(timeout);
      task.removeListener('touch', touchHandler);
      done(err);
    });
  }

  lock() {
    this.runningCount++;
  }

  unlock() {
    this.runningCount--;
  }

  get isLocked() {
    return this.runningCount >= this.options.concurrency;
  }
}

