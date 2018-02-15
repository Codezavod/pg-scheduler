
import * as EventEmitter from 'events';
import {
    BelongsToGetAssociationMixin, HasManyCountAssociationsMixin, HasManyCreateAssociationMixin,
    HasManyGetAssociationsMixin, Instance, Model, QueryTypes, Sequelize, SyncOptions,
} from 'sequelize';
import * as debugLog from 'debug';

const debug = debugLog('pg-scheduler:models');

export interface TaskAttribute {
    id: number;
    name: string;
    data: any;
    interval: number;
    nextRunAt: Date;
    startAt: Date;
    endAt: Date;
    concurrency: number;
    priority: number;
    timeout: number;
    failsCount: number;
    runAtTime: string;
    repeatOnError: boolean;
    createdAt: Date;
    updatedAt: Date;
    deletedAt: Date;
}

export interface TaskInstance extends Instance<TaskAttribute>, TaskAttribute {
    emitter: EventEmitter;
    getLocks: HasManyGetAssociationsMixin<LockInstance>;
    countLocks: HasManyCountAssociationsMixin;
    createLock: HasManyCreateAssociationMixin<LockAttribute, LockInstance>;
    Locks?: LockInstance[];
    checkEmitter(): void;
    on(...args: any[]): void;
    removeListener(...args: any[]): void;
    emit(...args: any[]): void;
    touch(...args: any[]): void;
}

export interface TaskModel extends Model<TaskInstance, TaskAttribute> {
    someClassMethod(): string;
}

export interface LockAttribute {
    id: number;
    workerName: string;
    createdAt: Date;
    updatedAt: Date;
    deletedAt: Date;
}

export interface LockInstance extends Instance<LockAttribute>, LockAttribute {
    getTask: BelongsToGetAssociationMixin<TaskInstance>;
    Task?: TaskInstance;
}

export interface LockModel extends Model<LockInstance, LockAttribute> { }

export class Models {
    public Task: TaskModel;
    public Lock: LockModel;

    constructor(private sequelize: Sequelize) {
        this.Task = this.taskModel() as any;
        this.Lock = this.lockModel() as any;

        this.setupRelationships();
    }

    public setupRelationships() {
        this.Task.hasMany(this.Lock, {onDelete: 'CASCADE'});
        this.Lock.belongsTo(this.Task);
    }

    public async sync(options?: SyncOptions) {
        await this.Task.sync(options);
        await this.Lock.sync(options);
    }

    private lockModel() {
        return this.sequelize.define<LockInstance, LockAttribute>('Lock', {
            workerName: {
                type: this.sequelize.Sequelize.STRING,
                allowNull: false,
                validate: {
                    notEmpty: true,
                },
            },
        }, {timestamps: true});
    }

    private taskModel() {
        const Task = this.sequelize.define<TaskInstance, TaskAttribute>('Task', {
            name: {
                type: this.sequelize.Sequelize.STRING,
                allowNull: false,
                validate: {
                    notEmpty: true,
                },
            },
            data: {
                type: this.sequelize.Sequelize.JSONB,
                defaultValue: {},
            },
            interval: {
                type: this.sequelize.Sequelize.INTEGER,
            },
            nextRunAt: {
                type: this.sequelize.Sequelize.DATE,
            },
            startAt: {
                type: this.sequelize.Sequelize.DATE,
            },
            endAt: {
                type: this.sequelize.Sequelize.DATE,
            },
            concurrency: {
                type: this.sequelize.Sequelize.INTEGER,
                defaultValue: 1,
            },
            priority: {
                type: this.sequelize.Sequelize.INTEGER,
                defaultValue: 0,
            },
            timeout: {
                type: this.sequelize.Sequelize.INTEGER,
                defaultValue: (1000 * 60 * 10), // 10 minutes
            },
            failsCount: {
                type: this.sequelize.Sequelize.INTEGER,
                defaultValue: 0,
            },
            runAtTime: {
                type: this.sequelize.Sequelize.TIME,
            },
            repeatOnError: {
                type: this.sequelize.Sequelize.BOOLEAN,
                defaultValue: false,
            },
        }, {
            timestamps: true,
        });

        (Task as any).prototype.checkEmitter = function(this: TaskInstance) {
            if (!this.emitter) {
                this.emitter = new EventEmitter();
            }
        };

        (Task as any).prototype.on = function(this: TaskInstance, ...args: any[]) {
            this.checkEmitter();
            (this.emitter.on as any)(...args);
        };

        (Task as any).prototype.removeListener = function(this: TaskInstance, ...args: any[]) {
            this.checkEmitter();
            (this.emitter.removeListener as any)(...args);
        };

        (Task as any).prototype.emit = function(this: TaskInstance, ...args: any[]) {
            this.checkEmitter();
            (this.emitter.emit as any)(...args);
        };

        (Task as any).prototype.touch = async function(this: TaskInstance) {
            debug(`${process.pid} '.touch()' called for task ${this.name} (${this.id})`);
            this.emit('touch');

            await this.sequelize.query(`UPDATE "Locks" SET "updatedAt" = NOW() WHERE "TaskId" = :task_id`, {
                replacements: {
                    task_id: this.id,
                },
                type: QueryTypes.UPDATE,
            });
        };

        return Task;
    }
}
