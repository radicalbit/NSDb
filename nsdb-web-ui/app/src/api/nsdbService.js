import { Observable } from 'rxjs';
import request from '../utils/request';

const httpEndpoint = process.env.REACT_APP_NSDB_HTTP;
const wsEndpoint = process.env.REACT_APP_NSDB_WS;

export function fetchDatabases() {
  return request(`${httpEndpoint}/commands/dbs`, {
    method: 'GET',
  });
}

export function fetchNamespaces(database) {
  return request(`${httpEndpoint}/commands/${database}/namespaces`, {
    method: 'GET',
  });
}

export function fetchMetrics(database, namespace) {
  return request(`${httpEndpoint}/commands/${database}/${namespace}/metrics`, {
    method: 'GET',
  });
}

export function fetchMetricDescription(database, namespace, metric) {
  return request(`${httpEndpoint}/commands/${database}/${namespace}/${metric}`, {
    method: 'GET',
  });
}

export function fetchHistoricalQuery(query) {
  return request(`${httpEndpoint}/query`, {
    headers: { 'Content-Type': 'application/json' },
    method: 'POST',
    body: JSON.stringify(query),
  });
}

export function runRealtimeQuery(id) {
  const url = window.location.href.replace('http', 'ws').replace('/ui/', '');
  return Observable.webSocket({
    url: `${url}/ws-stream`,
    openObserver: { next: () => console.log(`Opening socket for tab ${id}`) },
    closeObserver: { next: () => console.log(`Closing socket for tab ${id}`) },
    closingObserver: { next: () => console.log(`Closing socket for tab ${id}`) },
  });
}
