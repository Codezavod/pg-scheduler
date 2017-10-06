
import {Instance, Model, Sequelize} from 'sequelize';

declare var Scheduler: Scheduler.SchedulerStatic;

export = Scheduler;

declare namespace Scheduler {
    interface SchedulerStatic {
        new (options: Options): Scheduler;
    }

    interface Options {
        db?: {
            database: string,
            username: string,
            password: string,
            options?: {
                host?: string,
                dialect?: string,
                logging?: () => void | boolean,
            },
        },
        pollingInterval?: number, // milliseconds
        locksCheckingInterval?: number, // milliseconds
        workerName?: string,
        pollingWhereClause?: any,
        maxConcurrency?: number, // concurrency for current instance for ALL tasks
        client: Sequelize;
    }

    interface Scheduler {
        syncing: Promise<undefined>;
        models: Models;
        start(): Promise<undefined>;
        every(interval: number, taskName: string, data?: any, options?: TaskOptions): Promise<undefined>;
        everyDayAt(runAtTime: string, taskName: string, data?: any, options?: TaskOptions): Promise<undefined>;
        once(date: string | number | Date, taskName: string, data?: any, options?: TaskOptions): Promise<undefined>;
        totalProcessedCount: number;
        process(taskName: string, processor: (task: Task, done: (err?: Error) => void) => void, options?: ProcessorOptions): void;
        unDefineAll(): void;
        waitUntilAllEnd(): Promise<undefined>;
        startPolling(): void;
        startLocksChecking(): void;
        queueAddedHandler(): void;
        taskCompleteHandler(err: Error, task: Task, createdLock: Lock): void | Promise<undefined>;
        processTasks(): void;
        errorHandler(err: Error): void;
        stop(): Promise<undefined>;
    }

    interface Models {
        new(sequelize: Sequelize): Models;
        Task: Task;
        Lock: Lock;
    }

    interface ProcessorOptions {
        concurrency?: number;
        matchTask?: (task: Task) => boolean;
    }

    interface TaskOptions {
        startAt?: Date;
        endAt?: Date;
        concurrency?: number;
        priority?: number;
        timeout?: number;
        now?: boolean;
    }

    interface TaskInstance extends Instance<TaskAttribute> {
        checkEmitter(): void;
        on(...args: any[]): void;
        removeListener(...args: any[]): void;
        emit(...args: any[]): void;
        touch(): Promise<Lock>;
    }

    interface TaskAttribute {
        name: string;
        data?: any;
        interval?: string;
        nextRunAt?: Date | string | number;
        startAt?: Date | string | number;
        endAt?: Date | string | number;
        concurrency?: number;
        priority?: number;
        timeout?: number;
        failsCount?: number;
        runAtTime?: string;
    }

    interface Task extends Model<TaskInstance, TaskAttribute> {}

    interface LockInstance extends Instance<LockAttribute> {}

    interface LockAttribute {
        workerName: string;
    }

    interface Lock extends Model<LockInstance, LockAttribute> {}
}
