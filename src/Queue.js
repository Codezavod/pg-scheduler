
const EventEmitter = require('events'),
    _ = require('lodash'),
    debug = require('debug')('scheduler:queue');

class Queue extends EventEmitter {
    _queue = [];
    orderDirection = [];
    orderProperties = [];

    constructor(orderProperties = ['priority'], orderDirection = ['asc']) {
        super();

        this.orderProperties = orderProperties;
        this.orderDirection = orderDirection;
    }

    add(tasks) {
        debug(`tasks added to queue`);

        this.push(tasks);
        this.emit('added');
    }

    push(tasks) {
        if (!_.isArray(tasks)) {
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

        this._queue = _(newQueue).orderBy(this.orderProperties, this.orderDirection).value();
    }

    shift() {
        if (!this._queue.length) {
            this.emit('empty');

            return null;
        }

        return this._queue.shift();
    }
}

module.exports = Queue;
