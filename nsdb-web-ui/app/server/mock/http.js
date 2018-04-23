const path = require('path');
const chalk = require('chalk');
const jsonServer = require('json-server');
const server = jsonServer.create();
const router = jsonServer.router(path.join(__dirname, 'db.json'));
const middlewares = jsonServer.defaults();
const db = router.db;

// Set default middlewares (logger, static, cors and no-cache)
server.use(middlewares);
server.use(jsonServer.bodyParser);

//  Enable CORS on all requests
server.all('*', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'OPTIONS, GET, POST, PUT, DELETE');
  res.header(
    'Access-Control-Allow-Headers',
    [
      'Access-Control-Allow-Origin',
      'Access-Control-Allow-Credentials',
      'Accept',
      'Origin',
      'Content-type',
      'Access-token',
    ].join(',')
  );
  res.header('Access-Control-Allow-Credentials', 'true');
  res.header('Access-Control-Expose-Headers', 'Access-token');
  next();
});

// Custom routes
server.get('/commands/*/namespaces', (req, res, next) => {
  const namespaces = db.get('namespaces');
  res.status('200').send(JSON.stringify({ namespaces }, null, 2));
});

server.get('/commands/*/:namespace/metrics', (req, res, next) => {
  const metrics = db
    .get('metrics')
    .filter(metric => metric.id === req.params.namespace)
    .reduce((acc, metric) => acc.concat(metric.metrics), []);
  res.status('200').send(JSON.stringify({ metrics }, null, 2));
});

server.get('/commands/*/:namespace/:metric', (req, res, next) => {
  const fields = db
    .get('descriptions')
    .filter(description => description.id === req.params.metric)
    .reduce((acc, description) => acc.concat(description.fields), []);
  res.status('200').send(JSON.stringify({ fields }, null, 2));
});

server.post('/query', (req, res, next) => {
  const { namespace, metric } = req.body;
  const key = `${namespace}/${metric}`;
  const records = db
    .get('queryResults')
    .filter(queryResult => queryResult.id === key)
    .reduce((acc, queryResult) => acc.concat(queryResult.records), []);
  res.status('200').send(JSON.stringify({ records }, null, 2));
});

// Use default router
server.use(router);

server.listen(4000, () => {
  chalk.green('JSON Server is running on 4000');
});
