# file-mysql-session
Express session store using the file system and backing up to a MySQL database

# Usage
```javascript
import * as session from 'express-session';
const FileMySQLSession = require('file-mysql-session')(session);

app.use(session({
    store: new FileMySQLSession({
        connection: pool, // MySQL connection pool
        dir: 'sessions', // dir path to keep session files
        createTable: true, // whether or not to create the table if it doesn not exist
        table: 'sessions', // the name of the table
        backupInterval: 60000 // the interval in which to update the database, in millis
    })
}))
```
