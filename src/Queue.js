
import EventEmitter from 'events';
import _ from 'lodash';
import _debug from 'debug';

const debug = _debug('scheduler:queue');

export default class Queue extends EventEmitter {
  _queue = [];
  order = [];
  iterates = [];

  constructor(iterates = ['priority'], order = ['asc']) {
    super();
    this.iterates = iterates;
    this.order = order;
  }

  add(tasks) {
    debug(`tasks added to queue`);
    this.push(tasks);
    this.emit('added');
  }

  push(tasks) {
    if(!_.isArray(tasks)) {
      tasks = [tasks];
    }

    debug(`"${tasks.length}" tasks pushed to queue`);

    let concatQueue = this._queue.concat(tasks),
        indexed = _.groupBy(concatQueue, 'id'),
        newQueue = [];

    _.each(indexed, function(tasks) {
      let task = tasks[0];
      newQueue = newQueue.concat(tasks.slice(0, task.concurrency));
    });

    this._queue = _(newQueue).orderBy(this.iterates, this.order).value();
  }

  shift() {
    if(!this._queue.length) {
      this.emit('empty');
      return null;
    }
    return this._queue.shift();
  }
}

