const debug = require('debug');

const log = debug('file-mysql-session:log');
log.error = debug('file-mysql-session:error');
log.debug = debug('file-mysql-session:debug');

module.exports = function (session) {
    const FileBackedUpSession = require('file-backedup-session')(session);

    function fileMySQLSession(options) {
        options = {
            dir: 'sessions',
            createTable: true,
            table: 'sessions',
            backupInterval: 60000,
            retryLimit: 100,
            retryWait: 100,
            log,
            ...options,
        }
        options.getSessions = () => new Promise((resolve, reject) => {
            options.connection.query(
                `SELECT session_id, data FROM ${options.table};`,
                (err, results) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(results);
                    }
                }
            )
        });
        options.deleteSessions = ids => new Promise((resolve, reject) => {
            options.connection.query(
                `DELETE FROM ${options.table} WHERE session_id IN (?);`,
                ids,
                err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        });
        options.insertOrUpdateSessions = sessions => new Promise((resolve, reject) => {
            options.connection.query(
                Array(sessions).fill(
                    `INSERT INTO ${options.table} (session_id, expires, data) VALUES (?,?,?)
                     ON DUPLICATE KEY UPDATE expires=?, data=?`
                ).join(';'),
                sessions.map(({id, expires, data}) => [
                    id,
                    expires,
                    data,
                    expires,
                    data,
                ]).reduce((a, b) => a.concat(b)),
                err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        });
        options.setupBackup = options.createTable && (() => new Promise((resolve, reject) => {
            options.connection.query(
                `CREATE TABLE IF NOT EXISTS ${options.table} (
                    session_id VARCHAR(128) PRIMARY KEY NOT NULL,
                    expires INT(11) UNSIGNED NOT NULL,
                    data TEXT
                );`,
                err => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        }));
        return new FileBackedUpSession(options);
    }

    return fileMySQLSession;
};
