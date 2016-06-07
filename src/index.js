
import EventEmitter from 'events';
import Sequelize from 'sequelize';
import _ from 'lodash';
import _debug from 'debug';
import Promise from 'bluebird';
import ProcessorsStorage from './ProcessorsStorage';
import Queue from './Queue';

const debug = _debug('scheduler');

let defaultOptions = {
  db: {
    database: 'scheduler',
    username: 'scheduler',
    password: 'scheduler',
    options: {
      host: 'localhost',
      dialect: 'postgres',
      logging: false
      // logging: console.log
    }
  },
  pollingInterval: 5000, // milliseconds
  locksCheckingInterval: 60000, // milliseconds
  workerName: process.pid
};
// how about concurrency per worker?

class Scheduler extends EventEmitter {
  options = {};
  sequelize = null;
  Task = null;
  Lock = null;
  queue = new Queue();
  static processorsStorage = new ProcessorsStorage();
  pollingTimeout = null;
  locksCheckingTimeout = null;
  // count of processed tasks for each task (success or fail)
  processedCount = {};

  constructor(options = {}) {
    super();
    this.options = _.defaultsDeep({}, options, defaultOptions);
    let dbOpts = this.options.db;
    this.sequelize = new Sequelize(dbOpts.database, dbOpts.username, dbOpts.password, dbOpts.options);
    this.defineModels();
  }

  start() {
    debug('start syncing');
    return this.sequelize.sync().then(() => {
      debug('sync completes successfully');
      this.processTasks();
      this.startPolling();
      this.startLocksChecking();
    });
  }

  defineModels() {
    this.Task = this.sequelize.define('Task', {
      name: {
        type: Sequelize.STRING,
        allowNull: false,
        validate: {
          notEmpty: true
        }
      },
      data: {
        type: Sequelize.JSONB,
        defaultValue: {}
      },
      interval: {
        type: Sequelize.INTEGER
      },
      nextRunAt: {
        type: Sequelize.DATE
      },
      startAt: {
        type: Sequelize.DATE
      },
      endAt: {
        type: Sequelize.DATE
      },
      concurrency: {
        type: Sequelize.INTEGER,
        defaultValue: 1
      },
      priority: {
        type: Sequelize.INTEGER,
        defaultValue: 0
      },
      timeout: {
        type: Sequelize.INTEGER,
        defaultValue: (1000 * 60 * 10) // 10 minutes
      },
      failsCount: {
        type: Sequelize.INTEGER,
        defaultValue: 0
      }
    });

    this.Lock = this.sequelize.define('Lock', {
      workerName: {
        type: Sequelize.STRING,
        allowNull: false,
        validate: {
          notEmpty: true
        }
      }
    });

    this.Task.hasMany(this.Lock, {onDelete: 'CASCADE'});
    this.Lock.belongsTo(this.Task);

    debug('models defined');
  }

  every(interval, taskName, data = {}, options = {}) {
    // TODO: human-readable format
    let nextRunAt = new Date(Date.now() + interval),
        {startAt, endAt, concurrency, priority, timeout, now} = options;

    if(now) {
      nextRunAt = new Date();
    }

    return this.Task.create({
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

  once(date, taskName, data = {}, options = {}) {
    // TODO: human-readable format
    let nextRunAt = new Date(date),
        {startAt, endAt, concurrency, priority, timeout, now} = options;

    if(now) {
      nextRunAt = new Date();
    }

    return this.Task.create({
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

  // TODO: rewrite to instance method
  /**
   * Define processor for task.
   * @param {String} taskName
   * @param {Function} processor
   */
  static process(taskName, processor) {
    if(!taskName || !processor) {
      throw new Error('`taskName` and `processor` arguments are required');
    }

    debug(`adding ${taskName} to processors storage`);
    Scheduler.processorsStorage.add(taskName, processor);
  }

  // TODO: rewrite to instance method
  static unDefineAll() {
    Scheduler.processorsStorage.disableAll();
  }

  unDefineAll = Scheduler.unDefineAll;

  // TODO: rewrite to instance method
  static waitUntilAllEnd() {
    return new Promise((resolve) => {
      let interval = setInterval(() => {
        if(Scheduler.processorsStorage.runningCount() == 0) {
          clearInterval(interval);
          setImmediate(() => {
            resolve();
          });
        }
      }, 500);
    });
  }

  waitUntilAllEnd = Scheduler.waitUntilAllEnd;

  startPolling() {
    debug('starting polling');
    let pollingFunction = () => {
      // prevent concurrency queries
      clearTimeout(this.pollingTimeout);
      let currDate = new Date();
      this.Task.findAll({
        where: {
          nextRunAt: {$lte: currDate},
          startAt: {$or: {$lte: currDate, $eq: null}},
          endAt: {$or: {$gte: currDate, $eq: null}}
        },
        order: [['priority', 'ASC']],
        include: [ this.Lock ]
      }).then((foundTasks) => {
        foundTasks = _.reject(foundTasks, (task) => task.Locks.length >= task.concurrency);
        debug(`found ${foundTasks.length} tasks`);

        this.queue.add(foundTasks);

        this.pollingTimeout = setTimeout(pollingFunction, this.options.pollingInterval);
      }).catch(this.errorHandler.bind(this));
    };

    this.pollingTimeout = setTimeout(pollingFunction, this.options.pollingInterval);
  }

  startLocksChecking() {
    debug('starting locks checking');
    let pollingFunction = () => {
      // prevent concurrency queries
      clearTimeout(this.locksCheckingTimeout);
      let currDate = new Date();
      this.Lock.findAll({ include: [ this.Task ] }).then((foundLocks) => {
        return Promise.resolve(foundLocks).map((lock) => {
          if(!lock.Task) {
            return Promise.resolve();
          }

          if(currDate.getTime() - new Date(lock.updatedAt).getTime() > lock.Task.timeout) {
            debug(`lock ${lock.id} for ${lock.Task.id} (${lock.Task.name}) expired. removing`);
            return lock.destroy();
          }

          return Promise.resolve();
        }, {concurrency: 1}).then(() => {
          this.locksCheckingTimeout = setTimeout(pollingFunction, this.options.locksCheckingInterval);
        });
      }).catch(this.errorHandler.bind(this));
    };

    this.locksCheckingTimeout = setTimeout(pollingFunction, this.options.locksCheckingInterval);
  }

  queueAddedHandler() {
    let task, noProcessors = [];
    // debug('queue added', this.queue);
    while(task = this.queue.shift()) {
      let taskRunningCount = Scheduler.processorsStorage.runningCount(task.name);
      // skip if concurrency limit reached
      // this is concurrency per-worker
      debug(`${taskRunningCount} worker locks found for task ${task.name}`);
      if(taskRunningCount >= task.concurrency) {
        debug('worker concurrency limit reached');
        noProcessors.push(task);
        continue;
      }

      let processor = Scheduler.processorsStorage.get(task.name);
      // skip if no free processors found
      if(!processor) {
        debug(`no processors for task "${task.name}"`);
        noProcessors.push(task);
        continue;
      }

      // TODO: lock expiration

      ((task, processor) => {
        return this.sequelize.transaction((t) => {
          // TODO: get table name from model definition
          return this.sequelize.query('LOCK TABLE "Locks" IN ACCESS EXCLUSIVE MODE', {transaction: t}).then(() => {
            return task.countLocks({transaction: t}).then((count) => {
              // this is overall concurrency
              debug(`${count} overall locks found for task ${task.name}`);
              if(count >= task.concurrency) {
                debug('overall concurrency reached');
                this.queue.push(task);
                return;
              }

              // lock processor in worker
              processor.lock();

              return task.createLock({workerName: this.options.workerName}, {transaction: t}).then((createdLock) => {
                debug(`lock ${createdLock.id} created for task ${task.name}. start processor`);
                processor.start(task, (err) => {
                  if(!this.processedCount[task.id]) {
                    this.processedCount[task.id] = 0;
                  }
                  this.processedCount[task.id]++;

                  if(err) {
                    task.failsCount++;
                    debug('processor completes with error', err);
                  } else {
                    task.failsCount = 0;
                  }

                  if(!task.interval) {
                    // remove task created with `.once()`. lock will be removed with CASCADE
                    return task.destroy().then(() => {
                      this.emit(`task-${task.name}-complete`);
                    }).catch(this.errorHandler.bind(this));
                  }

                  task.nextRunAt = new Date(Date.now() + task.interval);

                  task.save().then(() => {
                    debug('task saved. removing lock');
                    return this.Lock.destroy({where: {id: createdLock.id}});
                  }).then(() => {
                    this.emit(`task-${task.name}-complete`);
                  }).catch(this.errorHandler.bind(this));
                });
              });
            });
          });
        }).catch(this.errorHandler.bind(this));
      })(task, processor);
    }

    if(noProcessors.length) {
      this.queue.push(noProcessors);
    }
  }

  processTasks() {
    // `added` event not emit on `.push()` to queue
    let queueAddedHandlerDeBounced = _.debounce(this.queueAddedHandler, this.options.pollingInterval / 2, {leading: true, trailing: false});
    this.queue.on('added', queueAddedHandlerDeBounced.bind(this));
  }

  errorHandler(err) {
    if(this.listeners('error').length) {
      this.emit('error', err);
    } else {
      // let it crash?
      throw err;
    }
  }

  stop() {
    debug('stop called');
    this.queue.removeListener('added', this.queueAddedHandler);
    clearTimeout(this.pollingTimeout);
    clearTimeout(this.locksCheckingTimeout);
    return this.Lock.destroy({where: {workerName: this.options.workerName.toString()}}).then(() => {
      return this.sequelize.close();
    });
  }
}

export default Scheduler;
