
import sinon from 'sinon';
import Scheduler from '../src/index';

function defer() {
  var resolve, reject;
  var promise = new Promise(function() {
    resolve = arguments[0];
    reject = arguments[1];
  });
  return {
    resolve: resolve,
    reject: reject,
    promise: promise
  };
}

describe('Scheduler', () => {
  it('should export function', () => {
    Scheduler.should.be.a.Function();
  });

  it('should have a `.calcNextRunAt` static method', () => {
    Scheduler.should.have.property('calcNextRunAt').which.is.a.Function();
  });

  it('should have a `.assertRunAtTime` static property', () => {
    Scheduler.should.have.property('assertRunAtTime').which.is.a.Function();
  });
});

describe('Instance', () => {
  let instance,
      startPromise;

  before(() => {
    instance = new Scheduler();
    return instance.syncing;
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

  it('should have a `.Task` property', () => {
    instance.should.have.property('Task').which.is.a.Object();
  });

  it('should `.start()` method return a promise', () => {
    (startPromise = instance.start()).should.be.a.Promise();
  });
});

describe('Processing', () => {
  let instance;
  before(() => {
    instance = new Scheduler({pollingInterval: 500});
    return instance.syncing;
  });
  after(() => {
    return instance.Task.destroy({where: {$or: [{name: 'task'}, {name: 'task4'}, {name: 'task5'}]}}).then(() => {
      instance.stop();
    });
  });

  it('should start', () => {
    return instance.start();
  });

  it('should create new Task', () => {
    return instance.once(new Date(), 'task', {
      qwe: 'asd'
    });
  });

  it('should add new processor', (done) => {
    instance.on('task-task-complete', () => {
      done();
    });
    instance.process('task', (task, cb) => {
      task.should.have.properties(['data', 'name']);
      cb();
    });
  });

  it('should create new task and add 3 processors', (done) => {
    let defers = [defer(), defer(), defer()],
        promises = [defers[0].promise, defers[1].promise, defers[2].promise];

    for(let i = 0; i < 3; i++) {
      instance.process('task2', (task, cb) => {
        // console.log(`processing-${i}`, task.get({plain: true}));
        task.should.have.properties(['data', 'name']);
        cb();
        defers[i].resolve();
      });
    }

    Promise.all(promises).then(() => {
      return instance.Task.destroy({where: {name: 'task2'}}).then(() => {
        done();
      });
    });

    instance.every(100, 'task2', {
      qwe: 'asd'
    });
  });
  
  it('should respect concurrency', (done) => {
    let timesProcessed = 0;

    for(let i = 0; i < 3; i++) {
      instance.process('task3', (task, cb) => {
        timesProcessed++;
        // console.log('start processing task3 with processor', i);
        task.should.have.properties(['data', 'name']);
        setTimeout(() => {
          // console.log('stop processing task3 with processor', i);
          cb();
        }, 1000);
      });
    }

    setTimeout(() => {
      if(timesProcessed > 2) {
        return done(new Error('concurrency has no respect'));
      }

      return instance.Task.destroy({where: {name: 'task3'}}).then(() => {
        done();
      });
    }, 2500);

    instance.every(100, 'task3', {
      qwe: 'asd'
    }, {concurrency: 1});
  });

  it('should throw on invalid `runAtTime` format', () => {
    (function() {
      instance.everyDayAt('2016-07-05', 'task4', {qwe: 'asd'});
    }).should.throw('`runAtTime` should be in format: "HH:mm" or "HH:mm:ss"');
  });

  it('should calculate `nextRunAt` with `runAtTime`', () => {
    let clock = sinon.useFakeTimers(new Date('2016-07-05 22:02:50').getTime());
    return instance.everyDayAt('00:00', 'task4', {qwe: 'asd'}).then((createdTask) => {
      createdTask.nextRunAt.getTime().should.be.equal(new Date('2016-07-06 00:00:00').getTime());
      clock.restore();
    });
  });

  it('should calculate `nextRunAt` with `interval`', () => {
    let clock = sinon.useFakeTimers(new Date('2016-07-05 22:02:50').getTime());
    return instance.every((1000 * 60 * 5), 'task5', {qwe: 'asd'}).then((createdTask) => {
      createdTask.nextRunAt.getTime().should.be.equal(new Date('2016-07-05 22:07:50').getTime());
      clock.restore();
    });
  });

});
