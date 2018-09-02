const util = require('util');
const fs = require('fs-extra');
const path = require('path');
const debug = require('debug');

const log = debug('file-mysql-session:log');
log.error = debug('file-mysql-session:error');

module.exports = function(session) {
    function FileMySqlSession(options) {
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
            cb(err, err || files.reduce((acc, id) => {
                if (id != 'updates') {
                    let session;
                    while (!session) {
                        try {
                            session = fs.readJsonSync(path.join(this.options.dir, id));
                        } catch (e) {}
                    }
                    acc[id] = session;
                }
                return acc;
            }, {}));
        });
    }

    FileMySqlSession.prototype.destroy = function(id, cb) {
        fs.remove(path.join(this.options.dir, id), err => {
            if (!err) this.addUpdated();
            cb(err);
        });
    }

    FileMySqlSession.prototype.clear = function(cb) {
        fs.emptyDir(this.options.dir, err => {
            if (!err) this.addUpdated();
            cb(err);
        });
    }

    FileMySqlSession.prototype.length = function(cb) {
        fs.readdir(this.options.dir, (err, files) => cb(err, err || files.length));
    }

    FileMySqlSession.prototype.get = async function(id, cb) {
        const sessionFile = path.join(this.options.dir, id);
        if (!(await fs.pathExists(sessionFile))) {
            cb();
        } else {
            fs.readJson(sessionFile, (err, session) => err ? this.get(id, cb) : cb(null, session));
        }
    }

    FileMySqlSession.prototype.set = function(id, session, cb) {
        fs.outputJson(path.join(this.options.dir, id), session, err => {
            if (!err) this.addUpdated(id);
            cb(err);
        });
    }

    FileMySqlSession.prototype.touch = function(id, session, cb) {
        session.cookie.expires = new Date(Date.now() + session.cookie.originalMaxAge);
        fs.outputJson(path.join(this.options.dir, id), session, err => {
            if (!err) this.addUpdated(id);
            cb(err);
        });
    }

    FileMySqlSession.prototype.addUpdated = async function(id) {
        let updates = [];
        if (await fs.pathExists(this.options.updatesPath))
            updates = await fs.readJson(this.options.updatesPath);
        if (!updates.includes(id)) {
            if (id) updates.push(id);
            fs.writeJson(this.options.updatesPath, updates);
        }
    }

    FileMySqlSession.prototype.backupSessions = async function() {
        if (await fs.pathExists(this.options.updatesPath)) {
            const updates = await fs.readJson(this.options.updatesPath);
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
                if (updates.length) {
                    this.options.connection.query(
                        Array(updates.length).fill(
                            `INSERT INTO ${this.options.table} (session_id, expires, data) VALUES (?,?,?)
                            ON DUPLICATE KEY UPDATE expires=?, data=?`
                        ).join(';'),
                        updates.map(id => {
                            const session = sessions[id];
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
