
const _ = require('lodash'),
    defaultOptions = {
        concurrency: 1,
    };

class Processor {
    processorFunc;
    runningCount = 0;

    constructor(processorFunc, options) {
        this.processorFunc = processorFunc;
        this.options = _.defaultsDeep({}, options, defaultOptions);

        if (typeof this.options.matchTask === 'function') {
            this.matchTask = this.options.matchTask;
        }
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
            },
            doneHandler = _.once((err) => {
                this.unlock();
                clearTimeout(timeout);
                task.removeListener('touch', touchHandler);
                done(err);
            });

        task.on('touch', touchHandler);

        const processResult = this.processorFunc(_.cloneDeep(task), doneHandler);

        if (processResult && processResult.catch) {
            processResult.catch(doneHandler);
        }
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

module.exports = Processor;
