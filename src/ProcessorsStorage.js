
const _ = require('lodash'),
    Processor = require('./Processor'),
    debug = require('debug')('pg-scheduler:processors:storage');

class ProcessorsStorage {
    _processors = {};

    constructor() { }

    /**
     * Adds processorFunc to storage
     * @param {String} taskName
     * @param {Function} processorFunc
     * @param {Object} [options]
     */
    add(taskName, processorFunc, options) {
        debug(`task ${taskName} added to ProcessorsStorage`);

        if (!this._processors[taskName]) {
            this._processors[taskName] = {currentIndex: -1, processors: [], active: true};
        }

        this._processors[taskName].processors.push(new Processor(processorFunc, options));
    }

    get(task) {
        const taskName = task.name,
            processor = this._processors[taskName],
            processorsForTask = processor && processor.active ? processor.processors : [];

        if (!processorsForTask.length) {
            debug(`no processors for task ${taskName} found`);

            return null;
        }

        const nextIndex = processor.currentIndex + 1,
            free = _(processorsForTask).reject('isLocked').value();

        let resultProcessor;

        if (!free.length) {
            return null;
        } else if (nextIndex >= free.length) {
            processor.currentIndex = 0;

            resultProcessor = free[0];
        } else {
            processor.currentIndex = nextIndex;

            resultProcessor = free[nextIndex];
        }

        if (typeof resultProcessor.matchTask === 'function' && !resultProcessor.matchTask(task)) {
            return null;
        }

        return resultProcessor;
    }

    runningCount(taskName = null) {
        let processorsForTask = [];

        if (taskName) {
            processorsForTask = this._processors[taskName] ? this._processors[taskName].processors : null;
        } else {
            _.each(this._processors, (taskProcessor) => {
                processorsForTask = processorsForTask.concat(taskProcessor.processors || []);
            });
        }

        if (!processorsForTask || !processorsForTask.length) {
            return 0;
        }

        return _(processorsForTask).filter('isLocked').map('runningCount').sum();
    }

    disableAll() {
        _.each(this._processors, (taskProcessor) => {
            taskProcessor.active = false;
        });
    }

    get processors() {
        return this._processors;
    }
}

module.exports = ProcessorsStorage;
