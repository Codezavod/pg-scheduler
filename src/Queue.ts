
import * as EventEmitter from 'events';
import {groupBy, chain} from 'lodash';
import * as debugLog from 'debug';

import {TaskInstance} from './Models';

const debug = debugLog('pg-scheduler:queue');

export class Queue extends EventEmitter {
    private queue: TaskInstance[] = [];
    private orderDirection: string[] = [];
    private orderProperties: string[] = [];

    constructor(orderProperties = ['priority'], orderDirection = ['asc']) {
        super();

        this.orderProperties = orderProperties;
        this.orderDirection = orderDirection;
    }

    public add(tasks: TaskInstance | TaskInstance[]) {
        this.push(tasks);
        this.emit('added');
    }

    public push(tasks: TaskInstance | TaskInstance[]) {
        if (!Array.isArray(tasks)) {
            tasks = [tasks];
        }

        debug(`"${tasks.length}" tasks pushed to queue`);

        const concatQueue = this.queue.concat(tasks),
            indexed = groupBy(concatQueue, 'id');
        let newQueue: TaskInstance[] = [];

        for (const taskId in indexed) {
            if (Object.prototype.hasOwnProperty.call(indexed, taskId)) {
                const tasks = indexed[taskId],
                    task = tasks[0];

                newQueue = newQueue.concat(
                    chain(tasks).orderBy(this.orderProperties, this.orderDirection).value().slice(0, task.concurrency),
                );
            }
        }

        this.queue = chain(newQueue).orderBy(this.orderProperties, this.orderDirection).value();
    }

    public shift() {
        if (!this.queue.length) {
            this.emit('empty');

            return;
        }

        return this.queue.shift();
    }
}
