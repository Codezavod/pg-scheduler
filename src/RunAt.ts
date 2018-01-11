
import {TaskInstance} from './Models';

export class RunAt {
    public static calcNextRunAt(interval: number | null, runAtTime?: string, task?: TaskInstance) {
        const now = new Date();

        if (interval && !runAtTime) {
            return new Date(now.getTime() + interval);
        } else if (!interval && runAtTime) {
            const nextDate = new Date(now.getTime() + (1000 * 60 * 60 * 24)),
                timeArr = runAtTime.split(':');

            nextDate.setHours(~~timeArr[0], ~~timeArr[1], ~~timeArr[2]);

            return nextDate;
        } else if (task && task.repeatOnError) {
            return new Date(now.getTime() + (1000 * 10 * task.failsCount));
        } else {
            throw new Error('Not implemented. use or only `interval` or only `runAtTime`');
        }
    }

    public static assertRunAtTime(runAtTime: string) {
        if (typeof runAtTime !== 'string' || runAtTime.indexOf(':') === -1 || runAtTime.split(':').length < 2) {
            throw new Error('`runAtTime` should be in format: "HH:mm" or "HH:mm:ss"');
        }
    }
}
