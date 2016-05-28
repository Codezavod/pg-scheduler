
import _ from 'lodash';
import Processor from './Processor';
import _debug from 'debug';

const debug = _debug('scheduler:processors:storage');

export default class ProcessorsStorage {
  _processors = {};

  constructor() {

  }

  add(taskName, processorFunc) {
    debug(`task ${taskName} added to ProcessorsStorage`);
    if(!this._processors[taskName]) {
      this._processors[taskName] = {currentIndex: -1, processors: []};
    }

    this._processors[taskName].processors.push(new Processor(processorFunc));
  }

  get(taskName) {
    let processorsForTask = this._processors[taskName] ? this._processors[taskName].processors : null;
    if(!processorsForTask || !processorsForTask.length) {
      debug(`no processors for task ${taskName} found`);
      return null;
    }

    let nextIndex = this._processors[taskName].currentIndex + 1,
        free = _(processorsForTask).reject('running').value();

    if(!free.length) {
      return null;
    } else if(nextIndex >= free.length) {
      this._processors[taskName].currentIndex = 0;
      return free[0];
    } else {
      this._processors[taskName].currentIndex = nextIndex;
      return free[nextIndex];
    }
  }

  runningCount(taskName) {
    let processorsForTask = this._processors[taskName] ? this._processors[taskName].processors : null;
    if(!processorsForTask || !processorsForTask.length) {
      return 0;
    }

    return _(processorsForTask).filter('running').value().length;
  }
}

