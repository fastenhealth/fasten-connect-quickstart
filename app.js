'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const {
  upsertConnectionFromWebhook,
  recordExportTask,
  updateExportTaskStatus,
} = require('./db');

const app = module.exports = express();

const PORT = process.env.PORT || 3000;
const FASTEN_PUBLIC_ID = process.env.FASTEN_PUBLIC_ID;
const FASTEN_PRIVATE_KEY = process.env.FASTEN_PRIVATE_KEY;
const FASTEN_API_BASE_URL = (process.env.FASTEN_API_BASE_URL || 'https://api.connect.fastenhealth.com').replace(/\/$/, '');
const indexTemplatePath = path.join(__dirname, 'views', 'index.html');
const indexTemplate = fs.readFileSync(indexTemplatePath, 'utf8');

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

app.post('/webhook', async (req, res) => {
  const event = req.body;

  if (!event || typeof event !== 'object') {
    return res.status(400).json({ message: 'Invalid webhook payload' });
  }

  const eventType = event.type || event.event_type;
  if (!eventType) {
    return res.status(400).json({ message: 'Missing event type' });
  }

  try {
    switch (eventType) {
      case 'patient.connection_success': {
        const connectionPayload = event.data;
        if (!connectionPayload || typeof connectionPayload !== 'object') {
          return res.status(400).json({ message: 'Invalid connection payload' });
        }

        const storedConnection = await upsertConnectionFromWebhook(connectionPayload);

        try {
          const task = await triggerEhiExport(storedConnection);
          return res.status(200).json({
            status: 'ok',
            type: eventType,
            connection: storedConnection,
            task,
          });
        } catch (error) {
          console.error('Failed to initiate EHI export', error);
          return res.status(502).json({
            message: error.message || 'Failed to initiate EHI export',
            type: eventType,
            connection: storedConnection,
          });
        }
      }
      case 'patient.ehi_export_success': {
        const data = event.data || {};
        const taskId = data.task_id;
        if (!taskId) {
          return res.status(400).json({ message: 'Missing task_id in export success payload' });
        }

        const downloadLinks = Array.isArray(data.download_links)
          ? data.download_links.filter((link) => link && typeof link === 'object' && link.url)
          : [];

        if (!downloadLinks.length) {
          return res.status(400).json({ message: 'No download links provided in payload' });
        }

        try {
          const savedFiles = await downloadEhiExportFiles(taskId, downloadLinks);
          const task = await updateExportTaskStatus({
            taskId,
            status: 'success',
            orgConnectionId: data.org_connection_id || null,
          });

          return res.status(200).json({ status: 'ok', type: eventType, files: savedFiles, task });
        } catch (error) {
          console.error('Failed to download EHI export files', error);
          await updateExportTaskStatus({
            taskId,
            status: 'failed',
            orgConnectionId: data.org_connection_id || null,
          }).catch((updateErr) => {
            console.error('Failed to update task status after download failure', updateErr);
          });

          return res.status(500).json({
            message: error.message || 'Failed to download export files',
            type: eventType,
          });
        }
      }
      case 'patient.ehi_export_failed': {
        const data = event.data || {};
        const taskId = data.task_id;

        if (taskId) {
          await updateExportTaskStatus({
            taskId,
            status: 'failed',
            orgConnectionId: data.org_connection_id || null,
          }).catch((error) => {
            console.error('Failed to update task status for failure event', error);
          });
        }

        return res.status(200).json({ status: 'acknowledged', type: eventType });
      }
      default:
        return res.status(202).json({ status: 'ignored', type: eventType });
    }
  } catch (error) {
    const isClientError = /Missing|Invalid/.test(error.message || '');
    const statusCode = isClientError ? 400 : 500;
    const log = isClientError ? console.warn : console.error;
    log('Failed to process webhook', error);
    return res.status(statusCode).json({ message: error.message || 'Failed to process webhook' });
  }
});

app.get('*', (req, res) => {
  const html = renderTemplate(indexTemplate, {
    PUBLIC_ID: FASTEN_PUBLIC_ID,
  });

  res.type('html').send(html);
});

/* istanbul ignore next */
if (!module.parent) {
  app.listen(PORT, () => {
    console.log(`Fasten Connect quickstart listening on port ${PORT}`);
  });
}

function renderTemplate(template, variables) {
  return Object.keys(variables).reduce((acc, key) => {
    const token = `{{${key}}}`;
    const value = escapeHtml(String(variables[key] ?? ''));
    return acc.split(token).join(value);
  }, template);
}

function escapeHtml(value) {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function triggerEhiExport(connectionRecord) {
  if (!FASTEN_PRIVATE_KEY) {
    throw new Error('FASTEN_PRIVATE_KEY environment variable is required to initiate EHI exports');
  }

  if (!connectionRecord?.org_connection_id) {
    throw new Error('Missing org_connection_id for export request');
  }

  const endpoint = new URL('/v1/bridge/fhir/ehi-export', FASTEN_API_BASE_URL).toString();
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${FASTEN_PRIVATE_KEY}`,
    },
    body: JSON.stringify({
      org_connection_id: connectionRecord.org_connection_id,
    }),
  });

  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    if (!response.ok) {
      throw new Error(`Failed to initiate EHI export (status ${response.status})`);
    }
    throw new Error('Failed to parse export creation response');
  }

  if (!response.ok) {
    const message = payload?.message || payload?.error || `HTTP ${response.status}`;
    throw new Error(`Failed to initiate EHI export: ${message}`);
  }

  const taskId = payload?.task_id || payload?.data?.task_id;
  if (!taskId) {
    throw new Error('Export creation response missing task_id');
  }

  const status = payload?.status || 'pending';

  return recordExportTask({
    taskId,
    userId: connectionRecord.user_id ?? null,
    orgConnectionId: connectionRecord.org_connection_id,
    status,
  });
}

async function downloadEhiExportFiles(taskId, links) {
  if (!FASTEN_PRIVATE_KEY) {
    throw new Error('FASTEN_PRIVATE_KEY environment variable is required to download exports');
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const baseDir = path.join(__dirname, 'data', 'exports', taskId);
  fs.mkdirSync(baseDir, { recursive: true });

  const saved = [];

  for (let index = 0; index < links.length; index += 1) {
    const link = links[index];
    const url = link.url;

    let filename;
    try {
      const parsed = new URL(url);
      filename = path.basename(parsed.pathname) || null;
    } catch (error) {
      filename = null;
    }

    if (!filename) {
      const suffix = link.export_type ? `.${sanitizeExtension(link.export_type)}` : '.ndjson';
      filename = `${taskId}-${index + 1}${suffix}`;
    }

    const filePath = path.join(baseDir, `${timestamp}-${filename}`);
    await downloadFile(url, filePath);
    saved.push(filePath);
  }

  return saved;
}

async function downloadFile(url, destination) {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${FASTEN_PRIVATE_KEY}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download ${url}: ${response.status} ${response.statusText}`);
  }

  const arrayBuffer = await response.arrayBuffer();
  await fs.promises.writeFile(destination, Buffer.from(arrayBuffer));
}

function sanitizeExtension(value) {
  return value ? value.replace(/[^a-z0-9]/gi, '').toLowerCase() || 'ndjson' : 'ndjson';
}
