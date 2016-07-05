
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
  workerName: process.pid,
  pollingWhereClause: {},
  maxConcurrency: 20 // concurrency for current instance for ALL tasks
};
// how about concurrency per worker?

class Scheduler extends EventEmitter {
  options = {};
  sequelize = null;
  Task = null;
  Lock = null;
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
    this.defineModels();
  }

  start() {
    debug(`${process.pid} start syncing`);
    return this.sequelize.sync().then(() => {
      debug(`${process.pid} sync completes successfully`);
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
      },
      runAtTime: {
        type: Sequelize.TIME
      }
    }, {
      instanceMethods: {
        // bad, bad hack :(
        checkEmitter: function() {
          if(!this.emitter) {
            this.emitter = new EventEmitter();
          }
        },
        on: function(...args) {
          this.checkEmitter();
          this.emitter.on(...args);
        },
        removeListener: function(...args) {
          this.checkEmitter();
          this.emitter.removeListener(...args);
        },
        emit: function(...args) {
          this.checkEmitter();
          this.emitter.emit(...args);
        },
        touch: function() {
          debug(`${process.pid} '.touch()' called for task ${this.name} (${this.id})`);
          this.emit('touch');
          return this.getLocks().then((foundLocks) => {
            debug(`${process.pid} '.touch()' found ${foundLocks.length} locks for task ${this.name} (${this.id})`);
            return Promise.resolve(foundLocks).map((Lock) => {
              Lock.updatedAt = new Date();
              Lock.changed('updatedAt', true);
              return Lock.save();
            });
          });
        }
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

    debug(`${process.pid} models defined`);
  }

  every(interval, taskName, data = {}, options = {}) {
    // TODO: human-readable format
    let nextRunAt = Scheduler.calcNextRunAt(interval),
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

  everyDayAt(runAtTime, taskName, data = {}, options = {}) {
    Scheduler.assertRunAtTime(runAtTime);

    let nextRunAt = Scheduler.calcNextRunAt(null, runAtTime),
        {startAt, endAt, concurrency, priority, timeout, now} = options;

    if(now) {
      nextRunAt = new Date();
    }

    return this.Task.create({
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

  static calcNextRunAt(interval, runAtTime) {
    let now = new Date();
    if(interval && !runAtTime) {
      return new Date(now.getTime() + interval);
    } else if(!interval && runAtTime) {
      let nextDate = new Date(now.getTime() + (1000 * 60 * 60 * 24)),
          timeArr = runAtTime.split(':');

      nextDate.setHours(~~timeArr[0], ~~timeArr[1], ~~timeArr[2]);

      return nextDate;
    } else {
      throw new Error('Not implemented. use or only `interval` or only `runAtTime`');
    }
  }

  /**
   * Define processor for task.
   * @param {String} taskName
   * @param {Function} processor
   * @param {Object} [options]
   */
  process(taskName, processor, options = {}) {
    if(!taskName || !processor) {
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
        if(this.processorsStorage.runningCount() == 0) {
          clearInterval(interval);
          setImmediate(() => {
            resolve();
          });
        }
      }, 500);
    });
  }

  startPolling() {
    let repeat = () => {
      this.pollingTimeout = setTimeout(pollingFunction, this.options.pollingInterval);
    };
    let pollingFunction = () => {
      let runningCount = this.processorsStorage.runningCount();
      if(runningCount >= this.options.maxConcurrency) {
        debug(`${process.pid} maxConcurrency (${this.options.maxConcurrency}) limit reached (${runningCount}). delay polling`);
        return repeat();
      }

      let currDate = new Date(),
          defaultWhere = {
            nextRunAt: {$lte: currDate},
            startAt: {$or: {$lte: currDate, $eq: null}},
            endAt: {$or: {$gte: currDate, $eq: null}},
            name: {$in: Object.keys(this.processorsStorage.processors)}
          },
          where = _.defaultsDeep({}, this.options.pollingWhereClause, defaultWhere);

      // prevent concurrency queries
      clearTimeout(this.pollingTimeout);
      this.Task.findAll({
        where: where,
        // order: [['priority', 'ASC']], // order will be in queue anyway
        include: [ this.Lock ]
      }).then((foundTasks) => {
        if(this.stopping) {
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
    let repeat = () => {
      this.locksCheckingTimeout = setTimeout(pollingFunction, this.options.locksCheckingInterval);
    };
    let pollingFunction = () => {
      // prevent concurrency queries
      clearTimeout(this.locksCheckingTimeout);
      let currDate = new Date();
      this.Lock.findAll({ include: [ {model: this.Task, fields: ['id', 'name', 'timeout']} ] }).then((foundLocks) => {
        return Promise.resolve(foundLocks).map((lock) => {
          if(!lock.Task || this.stopping) {
            return Promise.resolve();
          }

          if(currDate.getTime() - new Date(lock.updatedAt).getTime() > lock.Task.timeout) {
            debug(`${process.pid} lock ${lock.id} for ${lock.Task.id} (${lock.Task.name}) expired. removing`);
            return lock.destroy();
          }

          return Promise.resolve();
        }, {concurrency: 1}).then(() => {
          if(this.stopping) {
            return;
          }

          repeat();
        });
      }).catch(this.errorHandler.bind(this));
    };

    repeat();
  }

  queueAddedHandler() {
    let task, noProcessors = [];
    // debug(`{process.pid} queue added', this.queu`);
    while(task = this.queue.shift()) {
      if(this.stopping) {
        break;
      }

      let taskRunningCount = this.processorsStorage.runningCount(task.name);
      // skip if concurrency limit reached
      // this is concurrency per-worker
      debug(`${process.pid} ${taskRunningCount} worker locks found for task ${task.name} (${task.id})`);
      if(taskRunningCount >= task.concurrency) {
        debug(`${process.pid} worker concurrency limit reached`);
        noProcessors.push(task);
        continue;
      }

      let processor = this.processorsStorage.get(task.name);
      // skip if no free processors found
      if(!processor) {
        debug(`${process.pid} no processors for task "${task.name} (${task.id})"`);
        noProcessors.push(task);
        continue;
      }

      ((task, processor) => {
        return this.sequelize.transaction({isolationLevel: Sequelize.Transaction.ISOLATION_LEVELS.SERIALIZABLE}, (t) => {
          if(this.stopping) {
            return Promise.resolve();
          }

          // TODO: get table name from model definition
          return this.sequelize.query('LOCK TABLE "Locks" IN ACCESS EXCLUSIVE MODE;', {transaction: t}).then(() => {
            if(this.stopping) {
              return Promise.resolve();
            }

            return task.countLocks({transaction: t}).then((count) => {
              if(this.stopping) {
                return Promise.resolve();
              }

              // this is overall concurrency
              debug(`${process.pid} ${count} overall locks found for task ${task.name} (${task.id})`);
              if(count >= task.concurrency) {
                debug(`${process.pid} overall concurrency reached`);
                this.queue.push(task);
                return Promise.resolve();
              }

              return task.createLock({workerName: this.options.workerName}, {transaction: t}).then((createdLock) => {
                if(processor.isLocked) {
                  debug(`${process.pid} processor already locked`);
                  this.queue.push(task);
                  return this.Lock.destroy({where: {id: createdLock.id}, transaction: t});
                }

                if(this.stopping) {
                  processor.unlock();
                  return this.Lock.destroy({where: {id: createdLock.id}, transaction: t});
                }

                debug(`${process.pid} lock ${createdLock.id} created for task ${task.name} (${task.id}). start processor`);
                processor.start(task, (err) => {
                  if(!this.processedCount[task.id]) {
                    this.processedCount[task.id] = 0;
                  }
                  this.processedCount[task.id]++;

                  if(err) {
                    console.error('processor completes with error', err);
                    task.failsCount++;
                  } else {
                    task.failsCount = 0;
                  }

                  if(!task.interval) {
                    // remove task created with `.once()`. lock will be removed with CASCADE
                    return task.destroy().then(() => {
                      this.emit(`task-${task.name}-complete`);
                    }).catch(this.errorHandler.bind(this));
                  }

                  task.nextRunAt = Scheduler.calcNextRunAt(task.interval, task.runAtTime);

                  task.save().then(() => {
                    debug(`${process.pid} task saved. removing lock`);
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
    debug(`${process.pid} stop called`);
    this.stopping = true;
    this.queue.removeListener('added', this.queueAddedHandler);
    clearTimeout(this.pollingTimeout);
    clearTimeout(this.locksCheckingTimeout);
    return this.Lock.destroy({where: {workerName: this.options.workerName.toString()}}).delay(1000).then(() => {
      return this.sequelize.close();
    });
  }

  static assertRunAtTime(runAtTime) {
    if(typeof runAtTime != 'string' || runAtTime.indexOf(':') == -1 || runAtTime.split(':').length < 2) {
      throw new Error('`runAtTime` should be in format: "HH:mm" or "HH:mm:ss"');
    }
  }
}

export default Scheduler;
