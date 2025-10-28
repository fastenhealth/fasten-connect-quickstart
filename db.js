'use strict';

const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();

const dataDir = path.join(__dirname, 'data');
const dbPath = path.join(dataDir, 'fasten-connect.sqlite');
fs.mkdirSync(dataDir, { recursive: true });

const db = new sqlite3.Database(dbPath);

db.serialize(() => {
  db.run('PRAGMA foreign_keys = ON');

  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      display_name TEXT NOT NULL,
      email TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS connections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER,
      org_connection_id TEXT NOT NULL UNIQUE,
      connection_status TEXT,
      platform_type TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS tasks (
      task_id TEXT PRIMARY KEY,
      user_id INTEGER,
      org_connection_id TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
    )
  `);

  db.run('CREATE INDEX IF NOT EXISTS idx_tasks_org_connection_id ON tasks(org_connection_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)');

});
seedDefaultUser();


function upsertConnectionFromWebhook(connection) {
  return new Promise((resolve, reject) => {
    if (!connection || typeof connection !== 'object') {
      return reject(new Error('Invalid connection payload'));
    }

    const {
      external_id: externalId,
      org_connection_id: orgConnectionId,
      connection_status: connectionStatus,
      platform_type: platformType,
    } = connection;

    if (!orgConnectionId) {
      return reject(new Error('Missing org_connection_id'));
    }

    const userId = parseUserIdFromExternalId(externalId);

    const params = [
      userId,
      orgConnectionId,
      normalizeValue(connectionStatus),
      normalizeValue(platformType),
    ];

    db.run(
      `INSERT INTO connections (
            user_id,
            org_connection_id,
            connection_status,
            platform_type
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(org_connection_id) DO UPDATE SET
            user_id=COALESCE(excluded.user_id, connections.user_id),
            connection_status=COALESCE(excluded.connection_status, connections.connection_status),
            platform_type=COALESCE(excluded.platform_type, connections.platform_type),
            created_at=CURRENT_TIMESTAMP
      `,
      params,
      (err) => {
        if (err) {
          return reject(err);
        }

        db.get(
          `SELECT id, user_id, org_connection_id, connection_status, platform_type, created_at
           FROM connections
           WHERE org_connection_id = ?`,
          [orgConnectionId],
          (selectErr, row) => {
            if (selectErr) {
              return reject(selectErr);
            }

            resolve(row);
          }
        );
      }
    );
  });
}

function recordExportTask({ taskId, userId, orgConnectionId, status }) {
  return new Promise((resolve, reject) => {
    if (!taskId) {
      return reject(new Error('Missing task_id'));
    }

    if (!orgConnectionId) {
      return reject(new Error('Missing org_connection_id'));
    }

    if (!status) {
      return reject(new Error('Missing task status'));
    }

    const params = [taskId, userId ?? null, orgConnectionId, status];

    db.run(
      `INSERT INTO tasks (task_id, user_id, org_connection_id, status)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(task_id) DO UPDATE SET
          user_id=COALESCE(excluded.user_id, tasks.user_id),
          org_connection_id=COALESCE(excluded.org_connection_id, tasks.org_connection_id),
          status=excluded.status,
          updated_at=CURRENT_TIMESTAMP
      `,
      params,
      (err) => {
        if (err) {
          return reject(err);
        }

        db.get(
          `SELECT task_id, user_id, org_connection_id, status, created_at, updated_at
           FROM tasks
           WHERE task_id = ?`,
          [taskId],
          (selectErr, row) => {
            if (selectErr) {
              return reject(selectErr);
            }

            resolve(row);
          }
        );
      }
    );
  });
}

function updateExportTaskStatus({ taskId, status, userId = null, orgConnectionId = null }) {
  return new Promise((resolve, reject) => {
    if (!taskId) {
      return reject(new Error('Missing task_id'));
    }

    if (!status) {
      return reject(new Error('Missing task status'));
    }

    const params = [status, userId ?? null, orgConnectionId ?? null, taskId];

    db.run(
      `UPDATE tasks
         SET status = ?,
             user_id = COALESCE(?, user_id),
             org_connection_id = COALESCE(?, org_connection_id),
             updated_at = CURRENT_TIMESTAMP
       WHERE task_id = ?`,
      params,
      function (err) {
        if (err) {
          return reject(err);
        }

        if (this.changes === 0) {
          if (!orgConnectionId) {
            return resolve(null);
          }

          recordExportTask({ taskId, userId, orgConnectionId, status })
            .then(resolve)
            .catch(reject);
          return;
        }

        db.get(
          `SELECT task_id, user_id, org_connection_id, status, created_at, updated_at
           FROM tasks
           WHERE task_id = ?`,
          [taskId],
          (selectErr, row) => {
            if (selectErr) {
              return reject(selectErr);
            }

            resolve(row);
          }
        );
      }
    );
  });
}

function normalizeValue(value) {
  return value === undefined || value === '' ? null : value;
}

function parseUserIdFromExternalId(externalId) {
  if (externalId === undefined || externalId === null || externalId === '') {
    return null;
  }

  const parsed = Number.parseInt(externalId, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function seedDefaultUser() {
  db.get('SELECT COUNT(*) AS count FROM users', (err, row) => {
    if (err) {
      console.error('Failed to inspect users table', err);
      return;
    }

    if (row?.count > 0) {
      return;
    }

    db.run(
      `INSERT INTO users (display_name, email) VALUES (?, ?)`,
      ['John Doe', 'john.doe@example.com'],
      (insertErr) => {
        if (insertErr) {
          console.error('Failed to seed default user', insertErr);
        }
      }
    );
  });
}

module.exports = {
  upsertConnectionFromWebhook,
  recordExportTask,
  updateExportTaskStatus,
};
