
import {chain} from 'lodash';
import * as debugLog from 'debug';

import {ProcessFunction, Processor, ProcessorOptions} from './Processor';
import {Dictionary} from 'lodash';
import {TaskInstance} from './Models';

const debug = debugLog('pg-scheduler:processors:storage');

export interface ProcessorsItem {
    currentIndex: number;
    processors: Processor[];
    active: boolean;
}

export class ProcessorsStorage {
    public processors: Dictionary<ProcessorsItem> = {};

    public add(taskName: string, processorFunc: ProcessFunction, options?: Partial<ProcessorOptions>) {
        debug(`task ${taskName} added to ProcessorsStorage`);

        if (!this.processors[taskName]) {
            this.processors[taskName] = {currentIndex: -1, processors: [], active: true};
        }

        this.processors[taskName].processors.push(new Processor(processorFunc, options));
    }

    public get(task: TaskInstance): Processor | null {
        const taskName = task.name,
            processor: ProcessorsItem | void = this.processors[taskName];

        if (!processor || !processor.processors || !processor.processors.length) {
            debug(`no processors for task ${taskName} found`);

            return null;
        }

        const nextIndex = processor.currentIndex + 1,
            free = chain(processor.processors).reject('isLocked').value();

        let resultProcessor: Processor;

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

    public runningCount(taskName?: string) {
        let processorsForTask: Processor[] = [];

        if (taskName) {
            processorsForTask = this.processors[taskName] && this.processors[taskName].processors;
        } else {
            for (const taskName in this.processors) {
                if (Object.prototype.hasOwnProperty.call(this.processors, taskName)) {
                    const taskProcessor = this.processors[taskName];

                    processorsForTask = processorsForTask.concat(taskProcessor.processors || []);
                }
            }
        }

        if (!processorsForTask || !processorsForTask.length) {
            return 0;
        }

        return chain(processorsForTask).map('runningCount').sum().value();
    }

    public disableAll() {
        for (const taskName in this.processors) {
            if (Object.prototype.hasOwnProperty.call(this.processors, taskName)) {
                const taskProcessor = this.processors[taskName];

                taskProcessor.active = false;
            }
        }
    }
}
