
const EventEmitter = require('events'),
    Sequelize = require('sequelize'),
    _ = require('lodash'),
    debug = require('debug')('pg-scheduler'),
    Promise = require('bluebird'),
    ProcessorsStorage = require('./ProcessorsStorage'),
    Queue = require('./Queue'),
    Models = require('./Models'),
    RunAt = require('./RunAt');

const defaultOptions = {
    db: {
        database: 'pg-scheduler',
        username: 'pg-scheduler',
        password: 'pg-scheduler',
        options: {
            host: 'localhost',
            dialect: 'postgres',
            logging: false,
            // logging: console.log,
        },
    },
    pollingInterval: 5000, // milliseconds
    locksCheckingInterval: 60000, // milliseconds
    workerName: process.pid,
    pollingWhereClause: {},
    maxConcurrency: 20, // concurrency for current instance for ALL tasks
};
// how about concurrency per worker?

class Scheduler extends EventEmitter {
    options = {};
    sequelize = null;
    queue = new Queue();
    processorsStorage = new ProcessorsStorage();
    pollingTimeout = null;
    locksCheckingTimeout = null;
    // count of processed tasks for each task (success or fail)
    processedCount = {};
    stopping = false;

    constructor(options = {}) {
        super();

        this.options = _.defaultsDeep({}, options, defaultOptions);

        let dbOpts = this.options.db;

        this.sequelize = new Sequelize(dbOpts.database, dbOpts.username, dbOpts.password, dbOpts.options);
        this.models = new Models(this.sequelize);
        this.syncing = this.sequelize.sync();
        debug(`${process.pid} start syncing`);
    }

    start() {
        return this.syncing.then(() => {
            debug(`${process.pid} sync completes successfully`);
            this.processTasks();
            this.startPolling();
            this.startLocksChecking();
        });
    }

    every(interval, taskName, data = {}, options = {}) {
        // TODO: human-readable format
        let nextRunAt = RunAt.calcNextRunAt(interval),
            {startAt, endAt, concurrency, priority, timeout, now} = options;

        if (now) {
            nextRunAt = new Date();
        }

        return this.models.Task.create({
            name: taskName,
            data,
            interval,
            nextRunAt,
            startAt,
            endAt,
            concurrency,
            priority,
            timeout
        });
    }

    everyDayAt(runAtTime, taskName, data = {}, options = {}) {
        RunAt.assertRunAtTime(runAtTime);

        let nextRunAt = RunAt.calcNextRunAt(null, runAtTime),
            {startAt, endAt, concurrency, priority, timeout, now} = options;

        if (now) {
            nextRunAt = new Date();
        }

        return this.models.Task.create({
            name: taskName,
            data,
            runAtTime,
            nextRunAt,
            startAt,
            endAt,
            concurrency,
            priority,
            timeout
        });
    }

    once(date, taskName, data = {}, options = {}) {
        // TODO: human-readable format
        let nextRunAt = new Date(date),
            {startAt, endAt, concurrency, priority, timeout, now} = options;

        if (now) {
            nextRunAt = new Date();
        }

        return this.models.Task.create({
            name: taskName,
            data,
            nextRunAt,
            startAt,
            endAt,
            concurrency,
            priority,
            timeout
        });
    }

    get totalProcessedCount() {
        return _(this.processedCount).values().sum();
    }

    /**
     * Define processor for task.
     * @param {String} taskName
     * @param {Function} processor
     * @param {Object} [options]
     */
    process(taskName, processor, options = {}) {
        if (!taskName || !processor) {
            throw new Error('`taskName` and `processor` arguments are required');
        }

        debug(`${process.pid} adding ${taskName} to processors storage`);
        this.processorsStorage.add(taskName, processor, options);
    }

    unDefineAll() {
        this.processorsStorage.disableAll();
    }

    waitUntilAllEnd() {
        return new Promise((resolve) => {
            let interval = setInterval(() => {
                if (this.processorsStorage.runningCount() === 0) {
                    clearInterval(interval);
                    setImmediate(resolve);
                }
            }, 500);
        });
    }

    startPolling() {
        const repeat = () => {
            this.pollingTimeout = setTimeout(pollingFunction, this.options.pollingInterval);
        };

        const pollingFunction = () => {
            // prevent concurrency queries
            clearTimeout(this.pollingTimeout);

            const runningCount = this.processorsStorage.runningCount();

            if (runningCount >= this.options.maxConcurrency) {
                debug(`${process.pid} maxConcurrency (${this.options.maxConcurrency}) limit reached (${runningCount}). delay polling`);
                return repeat();
            }

            const currDate = new Date(),
                defaultWhere = {
                    nextRunAt: {$lte: currDate},
                    startAt: {$or: {$lte: currDate, $eq: null}},
                    endAt: {$or: {$gte: currDate, $eq: null}},
                    name: {$in: Object.keys(this.processorsStorage.processors)}
                },
                where = _.defaultsDeep({}, this.options.pollingWhereClause, defaultWhere);

            this.models.Task.findAll({
                where: where,
                // order: [['priority', 'ASC']], // order will be in queue anyway
                include: [ this.models.Lock ]
            }).then((foundTasks) => {
                if (this.stopping) {
                    return;
                }

                foundTasks = _.reject(foundTasks, (task) => task.Locks.length >= task.concurrency);
                debug(`${process.pid} found ${foundTasks.length} tasks`);

                this.queue.add(foundTasks);

                repeat();
            }).catch(this.errorHandler.bind(this));
        };

        debug(`${process.pid} starting polling`);
        repeat();
    }

    startLocksChecking() {
        debug(`${process.pid} starting locks checking`);

        const repeat = () => {
            this.locksCheckingTimeout = setTimeout(pollingFunction, this.options.locksCheckingInterval);
        };

        const pollingFunction = () => {
            // prevent concurrency queries
            clearTimeout(this.locksCheckingTimeout);

            const currDate = new Date();

            this.models.Lock.findAll({ include: [ {model: this.models.Task, fields: ['id', 'name', 'timeout']} ] }).then((foundLocks) => {
                return Promise.resolve(foundLocks).each((lock) => {
                    if (!lock.Task || this.stopping) {
                        return Promise.resolve();
                    }

                    if (currDate.getTime() - new Date(lock.updatedAt).getTime() > lock.Task.timeout) {
                        debug(`${process.pid} lock ${lock.id} for ${lock.Task.id} (${lock.Task.name}) expired. removing`);

                        return lock.destroy();
                    }

                    return Promise.resolve();
                }).then(() => {
                    if (this.stopping) {
                        return;
                    }

                    repeat();
                });
            }).catch(this.errorHandler.bind(this));
        };

        repeat();
    }

    queueAddedHandler() {
        let noProcessors = [];

        const recursive = (task) => {
            if (this.stopping || !task) {
                return;
            }

            let taskRunningCount = this.processorsStorage.runningCount(task.name);

            // skip if concurrency limit reached
            // this is concurrency per-worker
            debug(`${process.pid} ${taskRunningCount} worker locks found for task ${task.name} (${task.id})`);

            if (taskRunningCount >= task.concurrency) {
                debug(`${process.pid} worker concurrency limit reached`);
                noProcessors.push(task);
                recursive(this.queue.shift());

                return;
            }

            const processor = this.processorsStorage.get(task.name);
            // skip if no free processors found
            if (!processor) {
                debug(`${process.pid} no processors for task "${task.name} (${task.id})"`);
                noProcessors.push(task);
                recursive(this.queue.shift());

                return;
            }

            return this.sequelize.transaction({isolationLevel: Sequelize.Transaction.ISOLATION_LEVELS.SERIALIZABLE}, (t) => {
                if (this.stopping) {
                    return Promise.resolve();
                }

                return task.countLocks({transaction: t}).then((count) => {
                    if (this.stopping) {
                        return Promise.resolve();
                    }

                    // this is overall concurrency
                    debug(`${process.pid} ${count} overall locks found for task ${task.name} (${task.id})`);

                    if (count >= task.concurrency) {
                        debug(`${process.pid} overall concurrency reached`);
                        this.queue.push(task);

                        return Promise.resolve();
                    }

                    return task.createLock({workerName: this.options.workerName}, {transaction: t}).then((createdLock) => {
                        if (processor.isLocked) {
                            debug(`${process.pid} processor already locked`);
                            this.queue.push(task);

                            return this.models.Lock.destroy({where: {id: createdLock.id}, transaction: t});
                        }

                        if (this.stopping) {
                            processor.unlock();

                            return this.models.Lock.destroy({where: {id: createdLock.id}, transaction: t});
                        }

                        debug(`${process.pid} lock ${createdLock.id} created for task ${task.name} (${task.id}). start processor`);

                        try {
                            processor.start(task, (err) => {
                                this.taskCompleteHandler(err, task, createdLock);
                            });
                        } catch(err) {
                            this.taskCompleteHandler(err, task, createdLock);
                        }

                        recursive(this.queue.shift());
                    });
                });
            }).catch(this.errorHandler.bind(this));
        };

        recursive(this.queue.shift());

        if (noProcessors.length) {
            this.queue.push(noProcessors);
        }
    }

    taskCompleteHandler(err, task, createdLock) {
        if (!this.processedCount[task.id]) {
            this.processedCount[task.id] = 0;
        }
        this.processedCount[task.id]++;

        if (err) {
            console.error('processor completes with error', err);
            task.failsCount++;
        } else {
            task.failsCount = 0;
        }

        if (!task.interval && !task.runAtTime) {
            // remove task created with `.once()`. lock will be removed with CASCADE
            return task.destroy().then(() => {
                this.emit(`task-${task.name}-complete`);
            }).catch(this.errorHandler.bind(this));
        }

        task.nextRunAt = RunAt.calcNextRunAt(task.interval, task.runAtTime);

        task.save().then(() => {
            debug(`${process.pid} task saved. removing lock`);
            return this.models.Lock.destroy({where: {id: createdLock.id}});
        }).then(() => {
            this.emit(`task-${task.name}-complete`);
        }).catch(this.errorHandler.bind(this));
    }

    processTasks() {
        // `added` event not emit on `.push()` to queue
        const queueAddedHandlerDeBounced = _.debounce(this.queueAddedHandler, this.options.pollingInterval / 2, {leading: true, trailing: false});

        this.queue.on('added', queueAddedHandlerDeBounced.bind(this));
    }

    errorHandler(err) {
        if (this.listeners('error').length) {
            this.emit('error', err);
        } else {
            // let it crash?
            throw err;
        }
    }

    stop() {
        debug(`${process.pid} stop called`);

        this.stopping = true;
        this.queue.removeListener('added', this.queueAddedHandler);

        clearTimeout(this.pollingTimeout);
        clearTimeout(this.locksCheckingTimeout);

        return this.models.Lock.destroy({where: {workerName: this.options.workerName.toString()}}).delay(1000).then(() => {
            return this.sequelize.close();
        });
    }
}

module.exports = Scheduler;
