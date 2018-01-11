
import {once, cloneDeep, uniqueId, Dictionary} from 'lodash';

import {TaskInstance} from './Models';

const defaultOptions = {
    concurrency: 1,
};

export interface ProcessorOptions {
    matchTask?: MatchTaskFunction;
    concurrency: number;
}

export type NoArgsHandler = (...args: any[]) => void;
export type WithErrorHandler = (err?: Error) => void;
export type ProcessFunction = (task: TaskInstance, done: WithErrorHandler) => Promise<void> | void;
export type MatchTaskFunction = (task: TaskInstance) => boolean;

export class Processor {
    public matchTask?: MatchTaskFunction;

    private processorFunc: ProcessFunction;
    private runningCount = 0;
    private options: ProcessorOptions;
    private taskTimeouts: Dictionary<NodeJS.Timer> = {};

    constructor(processorFunc: ProcessFunction, options?: Partial<ProcessorOptions>) {
        this.processorFunc = processorFunc;
        this.options = Object.assign({}, defaultOptions, options);

        if (typeof this.options.matchTask === 'function') {
            this.matchTask = this.options.matchTask;
        }
    }

    public start(task: TaskInstance, done: WithErrorHandler) {
        this.lock();

        const randomId = uniqueId(),
            timeoutHandler = this.timeoutHandler(task, done).bind(this),
            touchHandler = this.touchHandler(randomId, task, timeoutHandler).bind(this),
            doneHandler = once<WithErrorHandler>(this.doneHandler(randomId, task, touchHandler, done).bind(this));

        this.resetTimeout(randomId, task, timeoutHandler);

        task.on('touch', touchHandler);

        const processResult = this.processorFunc(cloneDeep(task), doneHandler);

        if (processResult && processResult.catch) {
            processResult.catch(doneHandler);
        }
    }

    public lock() {
        this.runningCount++;
    }

    public unlock() {
        this.runningCount--;
    }

    private timeoutHandler(task: TaskInstance, done: WithErrorHandler): NoArgsHandler {
        return () => {
            console.error(`processor end with timeout (${task.timeout / 1000}sec) for task`, task.toJSON());
            this.unlock();
            done(new Error(`timeout ${task.timeout / 1000}sec reached.`));
        };
    }

    private touchHandler(randomId: string, task: TaskInstance, timeoutHandler: NoArgsHandler) {
        return () => {
            clearTimeout(this.taskTimeouts[randomId]);
            this.resetTimeout(randomId, task, timeoutHandler);
        };
    }

    private doneHandler(
        randomId: string,
        task: TaskInstance,
        touchHandler: NoArgsHandler,
        done: WithErrorHandler,
    ): WithErrorHandler {
        return (err?: Error) => {
            this.unlock();
            clearTimeout(this.taskTimeouts[randomId]);
            task.removeListener('touch', touchHandler);
            done(err);
        };
    }

    private resetTimeout(randomId: string, task: TaskInstance, timeoutHandler: NoArgsHandler) {
        this.taskTimeouts[randomId] = setTimeout(timeoutHandler, task.timeout);
    }

    get isLocked() {
        return this.runningCount >= this.options.concurrency;
    }
}
