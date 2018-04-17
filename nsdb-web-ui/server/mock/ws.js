const WebSocket = require('ws');

const wss = new WebSocket.Server({
  host: 'localhost',
  port: 4040,
  path: '/ws-stream',
});

wss.on('connection', (ws, req) => {
  const ip = req.connection.remoteAddress;
  console.log(`Accepted a new connection from IP: ${ip}`);

  const cancellable = setInterval(
    () =>
      ws.send(
        JSON.stringify({
          queryString: 'SELECT * FROM ... ORDER BY timestamp LIMIT 1',
          quid: '3bb06ef5-e09c-424d-a347-14a895d0f1a9',
          records: [
            {
              timestamp: Date.now(),
              value: Math.random(),
              dimensions: {
                'field-1-1-1': 'value-1-1-1',
                'field-1-1-2': 'value-1-1-2',
              },
            },
          ],
        })
      ),
    1000
  );

  ws.on('close', () => {
    console.log(`Closing connection for IP: ${ip}`);
    clearInterval(cancellable);
  });

  ws.on('error', error => {
    console.log(`An error occurred for connection with IP: ${ip}`);
    console.log(error);
    clearInterval(cancellable);
  });

  ws.on('message', msg => {
    console.log(msg);
  });
});
