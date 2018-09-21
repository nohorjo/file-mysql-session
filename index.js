const util = require('util');
const fs = require('fs-extra');
const path = require('path');
const debug = require('debug');
const { diff, applyChange } = require('deep-diff');
const freeze = require('deep-freeze');

const log = debug('file-mysql-session:log');
log.error = debug('file-mysql-session:error');
log.debug = debug('file-mysql-session:debug');

module.exports = function(session) {
    function FileMySqlSession(options) {
        log('init');
        this.options = {
            dir: 'sessions',
            createTable: true,
            table: 'sessions',
            backupInterval: 60000,
            ...options
        };
        this.options.updatesPath = path.join(this.options.dir, 'updates');

        const loadSessions = () => {
            this.options.connection.query(
                `SELECT session_id, data FROM ${this.options.table};`,
                (err, results) => {
                    if (err) {
                        log.error(err);
                        throw err;
                    }
                    results.forEach(s => fs.outputFile(path.join(this.options.dir, s.session_id), s.data));
                    log('init load sessions');
                }
            );
        };

        if (this.options.createTable) {
            this.options.connection.query(
                `CREATE TABLE IF NOT EXISTS ${this.options.table} (
                    session_id VARCHAR(128) PRIMARY KEY NOT NULL,
                    expires INT(11) UNSIGNED NOT NULL,
                    data TEXT
                );`,
                err => {
                    if (err) {
                        log.error(err);
                        throw err;
                    }
                    log('create table');
                    loadSessions();
                }
            );
        } else {
            loadSessions();
        }

        setInterval(() => this.backupSessions(), this.options.backupInterval);
    }

    FileMySqlSession.prototype.all = function(cb) {
        fs.readdir(this.options.dir, (err, files) => {
            log('get all');
            files = files.filter(f => f != 'updates');
            Promise.all(files.map(id => this._get(id)))
                .then(sessions => cb(null, sessions.reduce((acc, session, i) => {
                    acc[files[i]] = session;
                    return acc;
                }, {})))
                .catch(cb);
        });
    }

    FileMySqlSession.prototype.destroy = function(id, cb) {
        fs.remove(path.join(this.options.dir, id), err => {
            if (!err) this.addUpdated();
            log('destroy', id);
            cb(err);
        });
    }

    FileMySqlSession.prototype.clear = function(cb) {
        fs.emptyDir(this.options.dir, err => {
            if (!err) this.addUpdated();
            log('clear');
            cb(err);
        });
    }

    FileMySqlSession.prototype.length = function(cb) {
        log('length');
        fs.readdir(this.options.dir, (err, files) => cb(err, err || files.length));
    }

    FileMySqlSession.prototype._get = async function(id) {
        const sessionFile = path.join(this.options.dir, id);
        if (await fs.pathExists(sessionFile)) {
            try {
                return await fs.readJson(sessionFile);
            } catch (e) {
                if (e.message && e.message.match(/Unexpected.*JSON/)) return this._get(id);
                throw e;
            }
        }
    }

    FileMySqlSession.prototype.get = function(id, cb) {
        log('get', id);
        this._get(id).then(s => {
            log.debug('get', id, s);
            s.ORIGINAL = JSON.parse((JSON.stringify(s)));
            cb(null, s);
        }).catch(cb);
    }

    FileMySqlSession.prototype.set = function(id, session, cb) {
        const { ORIGINAL = {} } = session;
        delete session.ORIGINAL;
        const changes = diff(ORIGINAL, session);
        this._get(id).then(current => {
            delete current.ORIGINAL;
            changes.forEach(change => applyChange(current, true, change));
            fs.outputJson(path.join(this.options.dir, id), current, err => {
                if (!err) this.addUpdated(id);
                log('set', id);
                log.debug('set', id, session);
                cb(err);
            });
        }).catch(cb);
    }

    FileMySqlSession.prototype.touch = function(id, _, cb) {
        this._get(id).then(session => {
            session.cookie.expires = new Date(Date.now() + session.cookie.originalMaxAge);
            fs.outputJson(path.join(this.options.dir, id), session, err => {
                if (!err) this.addUpdated(id);
                log('touch', id);
                log.debug('touch', id, session);
                cb(err);
            });
        }).catch(cb);
    }

    FileMySqlSession.prototype.addUpdated = async function(id) {
        let updates = [];
        if (await fs.pathExists(this.options.updatesPath))
            try {
                updates = await fs.readJson(this.options.updatesPath);
            } catch (e) {}
        if (!updates.includes(id)) {
            if (id) updates.push(id);
            fs.writeJson(this.options.updatesPath, updates);
        }
        log('updates', updates);
    }

    FileMySqlSession.prototype.backupSessions = async function() {
        if (await fs.pathExists(this.options.updatesPath)) {
            let updates;
            try {
                updates = await fs.readJson(this.options.updatesPath);
            } catch (e) {
                return;
            }
            log('update db', updates);
            fs.remove(this.options.updatesPath);
            this.all((err, sessions) => {
                if (err) {
                    log.error(err);
                    throw err;
                }
                this.options.connection.query(
                    `DELETE FROM ${this.options.table} WHERE session_id NOT IN (?);`,
                    Object.keys(sessions),
                    err => {
                        if (err) {
                            log.error(err);
                            throw err;
                        }
                    }
                );
                updates = updates.filter(id => sessions[id]);
                if (updates.length) {
                    this.options.connection.query(
                        Array(updates.length).fill(
                            `INSERT INTO ${this.options.table} (session_id, expires, data) VALUES (?,?,?)
                            ON DUPLICATE KEY UPDATE expires=?, data=?`
                        ).join(';'),
                        updates.map(id => {
                            const session = sessions[id];
                            delete session.ORIGINAL;
                            const expires = ((new Date(session.cookie.expires) / 1000) | 0).toString();
                            return [
                                id,
                                expires,
                                JSON.stringify(session),
                                expires,
                                JSON.stringify(session),
                            ];
                        }).reduce((a, b) => a.concat(b)),
                        err => {
                            if (err) {
                                log.error(err);
                                throw err;
                            }
                        }
                    );
                }
            });
        }
    }

    util.inherits(FileMySqlSession, session.Store);
   
    return FileMySqlSession;
};
