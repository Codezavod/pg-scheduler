
import Sequelize, {FindOptions, Options, Sequelize as SequelizeType} from 'sequelize';
import {defaultsDeep, debounce, chain} from 'lodash';
import * as debugLog from 'debug';
import * as Bluebird from 'bluebird';

import {ProcessorsStorage} from './ProcessorsStorage';
import {Queue} from './Queue';
import {LockInstance, Models, TaskAttribute, TaskInstance} from './Models';
import {RunAt} from './RunAt';
import {NoArgsHandler, ProcessFunction, ProcessorOptions} from './Processor';

const debug = debugLog('pg-scheduler'),
    defaultOptions = {
        db: {
            database: 'pg-scheduler',
            username: 'pg-scheduler',
            password: 'pg-scheduler',
            options: {
                host: 'localhost',
                dialect: 'postgres',
                logging: false,
            },
        },
        pollingInterval: 5000, // milliseconds
        locksCheckingInterval: 60000, // milliseconds
        workerName: process.pid,
        pollingWhereClause: {},
        maxConcurrency: 20, // concurrency for current instance for ALL tasks
    };
// how about concurrency per worker?

export {LockInstance, TaskInstance} from './Models';

export interface DBOptions {
    database: string;
    username: string;
    password: string;
    options?: Options;
}

export interface SchedulerOptions extends InputSchedulerOptions {
    db?: DBOptions;
}

export interface InputSchedulerOptions {
    db?: Partial<DBOptions>;
    pollingInterval: number;
    locksCheckingInterval: number;
    workerName: string;
    pollingWhereClause: any;
    maxConcurrency: number;
    client?: SequelizeType;
    taskCompleteHandler: (task: TaskInstance) => void;
    errorHandler: (err: Error) => void;
}

export interface TaskOptions {
    startAt?: Date;
    endAt?: Date;
    concurrency?: number;
    priority?: number;
    timeout?: number;
    now?: boolean;
    repeatOnError?: boolean;
}

export class Scheduler {
    public options: SchedulerOptions;
    public sequelize: SequelizeType;
    public queue = new Queue();
    public processorsStorage = new ProcessorsStorage();
    public pollingTimeout: NodeJS.Timer;
    public locksCheckingTimeout: NodeJS.Timer;
    // count of processed tasks for each task (success or fail)
    public processedCount: {[taskId: number]: number} = {};
    public stopping = false;
    public models: Models;
    public syncing: Promise<void>;

    private boundPollingFunction = this.pollingFunction.bind(this);
    private boundLocksPollingFunction = this.locksPollingFunction.bind(this);
    private noProcessors: TaskInstance[] = [];
    private queueAddedHandlerDeBounced: NoArgsHandler;

    constructor(options: Partial<InputSchedulerOptions> = {}) {
        this.options = defaultsDeep({}, options, defaultOptions);

        if (this.options.client instanceof Sequelize) {
            this.sequelize = this.options.client;
        } else if (this.options.db) {
            const dbOpts = this.options.db;

            this.sequelize = new Sequelize(dbOpts.database, dbOpts.username, dbOpts.password, dbOpts.options);
        } else {
            throw new Error('one of `options.db` or `options.client` are required');
        }

        this.models = new Models(this.sequelize);
        this.syncing = this.models.sync();

        debug(`${process.pid} start syncing`);

        this.queueAddedHandlerDeBounced = debounce(
            this.queueAddedHandler,
            this.options.pollingInterval / 2,
            {leading: true, trailing: false},
        ).bind(this);
    }

    public async start() {
        await this.syncing;

        if (this.stopping) {
            return;
        }

        debug(`${process.pid} sync completes successfully`);

        this.processTasks();
        this.startPolling();
        this.startLocksChecking();
    }

    public every(
        interval: number,
        taskName: string,
        data: any = {},
        options: TaskOptions = {},
    ): Bluebird<TaskInstance> {
        // TODO: human-readable format
        let nextRunAt = RunAt.calcNextRunAt(interval);
        const {startAt, endAt, concurrency, priority, timeout, now} = options;

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
            timeout,
        } as any);
    }

    public everyDayAt(
        runAtTime: string,
        taskName: string,
        data: any = {},
        options: TaskOptions = {},
    ): Bluebird<TaskInstance> {
        RunAt.assertRunAtTime(runAtTime);

        let nextRunAt = RunAt.calcNextRunAt(null, runAtTime);
        const {startAt, endAt, concurrency, priority, timeout, now} = options;

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
            timeout,
        } as any);
    }

    public once(
        date: number | Date,
        taskName: string,
        data: any = {},
        options: TaskOptions = {},
    ): Bluebird<TaskInstance> {
        // TODO: human-readable format
        let nextRunAt = new Date(date instanceof Date ? date.getTime() : date);
        const {startAt, endAt, concurrency, priority, timeout, now, repeatOnError} = options;

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
            timeout,
            repeatOnError,
        } as any);
    }

    get totalProcessedCount() {
        return chain(this.processedCount).values().sum().value();
    }

    public process(taskName: string, processor: ProcessFunction, options?: Partial<ProcessorOptions>) {
        if (!taskName || !processor) {
            throw new Error('`taskName` and `processor` arguments are required');
        }

        debug(`${process.pid} adding ${taskName} to processors storage`);
        this.processorsStorage.add(taskName, processor, options);
    }

    public unDefineAll() {
        this.processorsStorage.disableAll();
    }

    public waitUntilAllEnd() {
        return new Promise((resolve) => {
            const interval = setInterval(() => {
                if (this.processorsStorage.runningCount() === 0) {
                    clearInterval(interval);
                    setImmediate(resolve);
                }
            }, 500);
        });
    }

    public async stop() {
        debug(`${process.pid} stop called`);

        this.stopping = true;
        this.queue.removeListener('added', this.queueAddedHandlerDeBounced);

        clearTimeout(this.pollingTimeout);
        clearTimeout(this.locksCheckingTimeout);

        await this.models.Lock.destroy({where: {workerName: this.options.workerName.toString()}}).delay(1000);
        await this.sequelize.close();
    }

    private pollingRepeat() {
        this.pollingTimeout = setTimeout(this.boundPollingFunction, this.options.pollingInterval);
    }

    private async pollingFunction() {
        // prevent concurrency queries
        clearTimeout(this.pollingTimeout);

        const currDate = new Date(),
            defaultWhere = {
                nextRunAt: {$lte: currDate},
                startAt: {$or: {$lte: currDate, $eq: null}},
                endAt: {$or: {$gte: currDate, $eq: null}},
                name: {$in: Object.keys(this.processorsStorage.processors)},
            },
            where = defaultsDeep({}, this.options.pollingWhereClause, defaultWhere),
            findOptions: FindOptions = {
                where,
                include: [ this.models.Lock ],
            };

        if (this.options.maxConcurrency) {
            findOptions.limit = this.options.maxConcurrency;
        }

        try {
            const foundTasks = (await this.models.Task.findAll(findOptions)).filter((task) => {
                return task.Locks && task.Locks.length < (task.concurrency || 1);
            });

            if (this.stopping) {
                return;
            }

            debug(`${process.pid} found ${foundTasks.length} tasks`);

            this.queue.add(foundTasks);

            this.pollingRepeat();
        } catch (err) {
            this.errorHandler(err);
        }
    }

    private startPolling() {
        debug(`${process.pid} starting polling`);
        this.pollingRepeat();
    }

    private locksPollingRepeat() {
        this.locksCheckingTimeout = setTimeout(this.boundLocksPollingFunction, this.options.locksCheckingInterval);
    }

    private async locksPollingFunction() {
        // prevent concurrency queries
        clearTimeout(this.locksCheckingTimeout);

        const currDate = new Date();

        try {
            const foundLocks = await this.models.Lock.findAll<TaskAttribute>({
                include: [
                    {model: this.models.Task, attributes: ['id', 'name', 'timeout']},
                ],
            });

            for (const lock of foundLocks) {
                if (!lock.Task || this.stopping) {
                    continue;
                }

                if (currDate.getTime() - new Date(lock.updatedAt).getTime() > lock.Task.timeout) {
                    debug(`${process.pid} lock ${lock.id} for ${lock.Task.id} (${lock.Task.name}) expired. removing`);

                    await lock.destroy();
                }
            }

            if (this.stopping) {
                return;
            }

            this.locksPollingRepeat();
        } catch (err) {
            this.errorHandler(err);
        }
    }

    private startLocksChecking() {
        debug(`${process.pid} starting locks checking`);

        this.locksPollingRepeat();
    }

    private async recursiveQueueAddHandler(task?: TaskInstance) {
        if (this.stopping || !task) {
            return;
        }

        const taskRunningCount = this.processorsStorage.runningCount(task.name),
            workerRunningCount = this.processorsStorage.runningCount();

        if (workerRunningCount >= this.options.maxConcurrency) {
            debug(
                `${process.pid} maxConcurrency (${this.options.maxConcurrency}) limit reached (${workerRunningCount})`,
            );
            this.noProcessors.push(task);
            this.recursiveQueueAddHandler(this.queue.shift());

            return;
        }

        // skip if concurrency limit reached
        // this is concurrency per-worker
        debug(`${process.pid} ${taskRunningCount} worker locks found for task ${task.name} (${task.id})`);

        if (taskRunningCount >= task.concurrency) {
            debug(`${process.pid} task concurrency limit reached (max: ${task.concurrency})`);
            this.noProcessors.push(task);
            this.recursiveQueueAddHandler(this.queue.shift());

            return;
        }

        const processor = this.processorsStorage.get(task);
        // skip if no free processors found
        if (!processor) {
            debug(`${process.pid} no processors for task "${task.name} (${task.id})"`);
            this.noProcessors.push(task);
            this.recursiveQueueAddHandler(this.queue.shift());

            return;
        }

        const t = await this.sequelize.transaction({
            isolationLevel: Sequelize.Transaction.ISOLATION_LEVELS.SERIALIZABLE,
        });

        try {
            if (this.stopping) {
                await t.rollback();
                return;
            }

            const locksCount = await task.countLocks({transaction: t} as any);

            if (this.stopping) {
                await t.rollback();
                return;
            }

            // this is overall concurrency
            debug(`${process.pid} ${locksCount} overall locks found for task ${task.name} (${task.id})`);

            if (locksCount >= task.concurrency) {
                debug(`${process.pid} overall concurrency reached`);
                this.queue.push(task);

                await t.rollback();
                return;
            }

            const createdLock = await task.createLock({workerName: this.options.workerName} as any, {transaction: t});

            if (processor.isLocked) {
                debug(`${process.pid} processor already locked`);
                this.queue.push(task);

                await t.rollback();
                return;
            }

            if (this.stopping) {
                processor.unlock();

                await t.rollback();
                return;
            }

            debug(`${process.pid} lock ${createdLock.id} created for task ${task.name} (${task.id}). start processor`);

            await t.commit();

            try {
                processor.start(task, (err) => {
                    this.taskCompleteHandler(err, task, createdLock);
                });
            } catch (err) {
                this.taskCompleteHandler(err, task, createdLock);
            }

            this.recursiveQueueAddHandler(this.queue.shift());
        } catch (err) {
            await t.rollback();
            this.errorHandler(err);
        }
    }

    private queueAddedHandler() {
        this.recursiveQueueAddHandler(this.queue.shift());

        if (this.noProcessors.length) {
            this.queue.push(this.noProcessors);
            this.noProcessors = [];
        }
    }

    private async taskCompleteHandler(err: Error | void, task: TaskInstance, createdLock: LockInstance) {
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

        if (
            !task.interval && !task.runAtTime &&
            (!task.repeatOnError || (task.repeatOnError && task.failsCount === 0))
        ) {
            // remove task created with `.once()`. lock will be removed with CASCADE
            try {
                await task.destroy();
                if (typeof this.options.taskCompleteHandler === 'function') {
                    this.options.taskCompleteHandler(task);
                }
                return;
            } catch (err) {
                this.errorHandler(err);
            }
        }

        task.nextRunAt = RunAt.calcNextRunAt(task.interval, task.runAtTime, task);

        try {
            await task.save();

            debug(`${process.pid} task saved. removing lock`);

            await this.models.Lock.destroy({where: {id: createdLock.id}});

            if (typeof this.options.taskCompleteHandler === 'function') {
                this.options.taskCompleteHandler(task);
            }
        } catch (err) {
            this.errorHandler(err);
        }
    }

    private processTasks() {
        // `added` event not emit on `.push()` to queue
        this.queue.on('added', this.queueAddedHandlerDeBounced);
    }

    private errorHandler(err: Error) {
        if (typeof this.options.errorHandler === 'function') {
            this.options.errorHandler(err);
        } else {
            // let it crash?
            throw err;
        }
    }
}
