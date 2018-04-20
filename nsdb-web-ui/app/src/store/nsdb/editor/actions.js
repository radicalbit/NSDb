import shortid from 'shortid';
import { types as TYPES } from './types';

/**
 * normal actions
 */

const addTab = () => ({
  type: TYPES.TAB_ADD,
  payload: {
    id: shortid.generate(),
    title: 'New Query',
  },
});

const removeTab = id => ({
  type: TYPES.TAB_REMOVE,
  payload: {
    id,
  },
});

const selectNamespace = (id, namespace) => ({
  type: TYPES.NAMESPACE_SELECT,
  payload: {
    id,
    namespace,
  },
});

const selectMetric = (id, metric) => ({
  type: TYPES.METRIC_SELECT,
  payload: {
    id,
    metric,
  },
});

/**
 * async actions
 */

const fetchNamespacesRequest = database => ({
  type: TYPES.NAMESPACES_FETCH_REQUEST,
  payload: {
    database,
  },
});

const fetchNamespacesSuccess = response => ({
  type: TYPES.NAMESPACES_FETCH_SUCCESS,
  payload: {
    response,
  },
});

const fetchNamespacesError = error => ({
  type: TYPES.NAMESPACES_FETCH_ERROR,
  error,
});

const fetchMetricsRequest = (database, namespace) => ({
  type: TYPES.METRICS_FETCH_REQUEST,
  payload: {
    database,
    namespace,
  },
});

const fetchMetricsSuccess = (namespace, response) => ({
  type: TYPES.METRICS_FETCH_SUCCESS,
  payload: {
    namespace,
    response,
  },
});

const fetchMetricsError = error => ({
  type: TYPES.METRICS_FETCH_ERROR,
  error,
});

const fetchMetricDescriptionRequest = (database, namespace, metric) => ({
  type: TYPES.METRIC_DESCRIPTION_FETCH_REQUEST,
  payload: {
    database,
    namespace,
    metric,
  },
});

const fetchMetricDescriptionSuccess = (namespace, metric, response) => ({
  type: TYPES.METRIC_DESCRIPTION_FETCH_SUCCESS,
  payload: {
    namespace,
    metric,
    response,
  },
});

const fetchMetricDescriptionError = error => ({
  type: TYPES.METRIC_DESCRIPTION_FETCH_ERROR,
  error,
});

export const actions = {
  addTab,
  removeTab,
  selectNamespace,
  selectMetric,
  fetchNamespacesRequest,
  fetchNamespacesSuccess,
  fetchNamespacesError,
  fetchMetricsRequest,
  fetchMetricsSuccess,
  fetchMetricsError,
  fetchMetricDescriptionRequest,
  fetchMetricDescriptionSuccess,
  fetchMetricDescriptionError,
};
