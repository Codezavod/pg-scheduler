
const EventEmitter = require('events'),
    Sequelize = require('sequelize'),
    Promise = require('bluebird'),
    debug = require('debug')('pg-scheduler:models');

class Models {
    constructor(sequelize) {
        this.sequelize = sequelize;

        this.Task = this.taskModel();
        this.Lock = this.lockModel();

        this.setupRelationships();
    }

    setupRelationships() {
        this.Task.hasMany(this.Lock, {onDelete: 'CASCADE'});
        this.Lock.belongsTo(this.Task);
    }

    lockModel() {
        return this.sequelize.define('Lock', {
            workerName: {
                type: Sequelize.STRING,
                allowNull: false,
                validate: {
                    notEmpty: true,
                },
            },
        });
    }

    taskModel() {
        return this.sequelize.define('Task', {
            name: {
                type: Sequelize.STRING,
                allowNull: false,
                validate: {
                    notEmpty: true,
                },
            },
            data: {
                type: Sequelize.JSONB,
                defaultValue: {},
            },
            interval: {
                type: Sequelize.INTEGER,
            },
            nextRunAt: {
                type: Sequelize.DATE,
            },
            startAt: {
                type: Sequelize.DATE,
            },
            endAt: {
                type: Sequelize.DATE,
            },
            concurrency: {
                type: Sequelize.INTEGER,
                defaultValue: 1,
            },
            priority: {
                type: Sequelize.INTEGER,
                defaultValue: 0,
            },
            timeout: {
                type: Sequelize.INTEGER,
                defaultValue: (1000 * 60 * 10), // 10 minutes
            },
            failsCount: {
                type: Sequelize.INTEGER,
                defaultValue: 0,
            },
            runAtTime: {
                type: Sequelize.TIME,
            },
        }, {
            instanceMethods: {
                // bad, bad hack :(
                checkEmitter: function() {
                    if (!this.emitter) {
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
                },
            },
        });
    }
}

module.exports = Models;
