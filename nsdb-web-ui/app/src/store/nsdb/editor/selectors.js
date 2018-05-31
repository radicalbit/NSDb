import createCachedSelector from 're-reselect';
import get from 'lodash/get';

const emptyNamespaces = [];
const emptyMetrics = [];
const emptyFields = [];

/**
 * normal selectors
 */

const getTabs = state => state.nsdb.editor.tabs;

const getDatabases = state => state.nsdb.editor.databases;

const getNamespaces = state => state.nsdb.editor.namespaces;

const getDescriptions = state => state.nsdb.editor.descriptions;

const getIsFetchingDatabases = state => state.nsdb.editor.isFetchingDatabases;

const getIsFetchingNamespaces = state => state.nsdb.editor.isFetchingNamespaces;

const getIsFetchingMetrics = state => state.nsdb.editor.isFetchingMetrics;

const getIsFetchingDescriptions = state => state.nsdb.editor.isFetchingDescriptions;

/**
 * computed selectors
 */

const getSelectedTab = createCachedSelector(
  getTabs,
  (state, props) => props.id,
  (tabs, id) => {
    return tabs.find(tab => tab.id === id);
  }
)((state, props) => props.id);

const getSelectedDatabase = (state, props) =>
  get(getSelectedTab(state, props), 'selectedDatabase', null);

const getSelectedNamespace = (state, props) =>
  get(getSelectedTab(state, props), 'selectedNamespace', null);

const getSelectedMetric = (state, props) =>
  get(getSelectedTab(state, props), 'selectedMetric', null);

const getDatabasesAllNames = state => getDatabases(state).allNames;

const getNamespacesByDatabase = createCachedSelector(
  getDatabases,
  getSelectedDatabase,
  (databases, selectedDatabase) => {
    return get(databases.byName[selectedDatabase], 'namespaces', emptyNamespaces);
  }
)((state, props) => props.id);

const getMetricsByNamespace = createCachedSelector(
  getNamespaces,
  getSelectedNamespace,
  (namespaces, selectedNamespace) => {
    return get(namespaces.byName[selectedNamespace], 'metrics', emptyMetrics);
  }
)((state, props) => props.id);

const getMetricDescription = createCachedSelector(
  getDescriptions,
  getSelectedNamespace,
  getSelectedMetric,
  (descriptions, selectedNamespace, selectedMetric) => {
    return get(
      descriptions.byName[`${selectedNamespace}/${selectedMetric}`],
      'fields',
      emptyFields
    );
  }
)((state, props) => props.id);

export const selectors = {
  getTabs,
  getIsFetchingDatabases,
  getIsFetchingNamespaces,
  getIsFetchingMetrics,
  getIsFetchingDescriptions,
  getSelectedDatabase,
  getSelectedNamespace,
  getSelectedMetric,
  getDatabasesAllNames,
  getNamespacesByDatabase,
  getMetricsByNamespace,
  getMetricDescription,
};
