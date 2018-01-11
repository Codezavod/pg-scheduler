
import 'should';
import * as sinon from 'sinon';
import {noop} from 'lodash';

import {Scheduler} from '../src/Scheduler';

function defer() {
    let resolve: (value?: any) => void = noop,
        reject: (reason?: any) => void = noop;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });

    return {
        resolve,
        reject,
        promise,
    };
}

const logFunction = (str: string) => {
    console.error(str);
};

process.on('unhandledRejection', e => { throw e; });

describe('Scheduler', () => {
    it('should export function', () => {
        Scheduler.should.be.a.Function();
    });
});

describe('Instance', () => {
    let instance: Scheduler,
        startPromise: Promise<void>;

    before(async () => {
        instance = new Scheduler({db: {options: {logging: logFunction}}});

        await instance.syncing;
        await instance.models.sync({force: true});
    });

    after(() => {
        return startPromise.then(() => {
            instance.stop();
        });
    });

    it('should have a `.start` method', () => {
        instance.should.have.property('start').which.is.a.Function();
    });

    it('should have a `.start` method', () => {
        instance.should.have.property('process').which.is.a.Function();
    });

    it('should have a `.start` method', () => {
        instance.should.have.property('processorsStorage').which.is.a.Object();
    });

    it('should have a `.stop` method', () => {
        instance.should.have.property('stop').which.is.a.Function();
    });

    it('should have a `.models` property', () => {
        instance.should.have.property('models').which.is.a.Object();
    });

    it('should `.start()` method return a promise', () => {
        (startPromise = instance.start()).should.be.a.Promise();
    });
});

describe('Processing', () => {
    let instance: Scheduler;

    before(() => {
        instance = new Scheduler({pollingInterval: 500, db: {options: {logging: logFunction}}});
        return instance.syncing;
    });

    after(async () => {
        await instance.models.Task.destroy({where: {$or: [{name: 'task'}, {name: 'task4'}, {name: 'task5'}]}});
        instance.stop();
    });

    it('should start', () => {
        return instance.start();
    });

    it('should create new Task', () => {
        return instance.once(new Date(), 'task', {
            qwe: 'asd',
        });
    });

    it('should add new processor', (done) => {
        instance.options.taskCompleteHandler = (task) => {
            if (task.name === 'task') {
                done();
            }
        };

        instance.process('task', (task, cb) => {
            task.should.have.properties(['data', 'name']);
            cb();
        });
    });

    it('should create new task and add 3 processors', (done) => {
        const defers = [defer(), defer(), defer()],
            promises = [defers[0].promise, defers[1].promise, defers[2].promise];

        for (let i = 0; i < 3; i++) {
            instance.process('task2', (task, cb) => {
                task.should.have.properties(['data', 'name']);
                cb();
                defers[i].resolve();
            });
        }

        Promise.all(promises).then(() => {
            return instance.models.Task.destroy({where: {name: 'task2'}}).then(() => {
                done();
            });
        }).catch(done);

        instance.every(100, 'task2', {
            qwe: 'asd',
        });
    });

    it('should respect concurrency', (done) => {
        let timesProcessed = 0;

        for (let i = 0; i < 3; i++) {
            instance.process('task3', (task, cb) => {
                timesProcessed++;
                task.should.have.properties(['data', 'name']);

                setTimeout(cb, 1000);
            });
        }

        setTimeout(() => {
            if (timesProcessed > 2) {
                return done(new Error('concurrency has no respect'));
            }

            return instance.models.Task.destroy({where: {name: 'task3'}}).then(() => {
                done();
            });
        }, 2500);

        instance.every(100, 'task3', {
            qwe: 'asd',
        }, {concurrency: 1});
    });

    it('should throw on invalid `runAtTime` format', () => {
        (() => {
            instance.everyDayAt('2016-07-05', 'task4', {qwe: 'asd'});
        }).should.throw('`runAtTime` should be in format: "HH:mm" or "HH:mm:ss"');
    });

    it('should calculate `nextRunAt` with `runAtTime`', () => {
        const clock = sinon.useFakeTimers(new Date('2016-07-05 22:02:50').getTime());

        return instance.everyDayAt('00:00', 'task4', {qwe: 'asd'}).then((createdTask) => {
            createdTask.nextRunAt.getTime().should.be.equal(new Date('2016-07-06 00:00:00').getTime());
            clock.restore();
        });
    });

    it('should calculate `nextRunAt` with `interval`', () => {
        const clock = sinon.useFakeTimers(new Date('2016-07-05 22:02:50').getTime());

        return instance.every((1000 * 60 * 5), 'task5', {qwe: 'asd'}).then((createdTask) => {
            createdTask.nextRunAt.getTime().should.be.equal(new Date('2016-07-05 22:07:50').getTime());
            clock.restore();
        });
    });
});

describe('Advanced concurrency', () => {
    let instance: Scheduler;

    before(async () => {
        instance = new Scheduler({pollingInterval: 500, db: {options: {logging: logFunction}}});
        await instance.syncing;
        await instance.start();
    });

    after(async () => {
        await instance.models.Task.destroy({where: {$or: [{name: 'task10'}]}});
        await instance.waitUntilAllEnd();
        await instance.stop();
    });

    it('should reuse processor function', (done) => {
        let timesProcessed = 0;

        instance.process('task10', (task, cb) => {
            timesProcessed++;
            task.should.have.properties(['data', 'name']);

            cb();
        }, {concurrency: 5});

        instance.every(100, 'task10', {
            qwe: 'asd',
        }, {concurrency: 5, timeout: 1500, now: true}).then(() => {
            setTimeout(() => {
                timesProcessed.should.be.equal(2);
                done();
            }, 1450);
        });
    });
});
