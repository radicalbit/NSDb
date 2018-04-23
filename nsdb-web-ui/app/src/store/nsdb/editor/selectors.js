import createCachedSelector from 're-reselect';
import get from 'lodash/get';

const emptyMetrics = [];
const emptyFields = [];

/**
 * normal selectors
 */

const getTabs = state => state.nsdb.editor.tabs;

const getNamespaces = state => state.nsdb.editor.namespaces;

const getDescriptions = state => state.nsdb.editor.descriptions;

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

const getSelectedNamespace = (state, props) =>
  get(getSelectedTab(state, props), 'selectedNamespace', null);

const getSelectedMetric = (state, props) =>
  get(getSelectedTab(state, props), 'selectedMetric', null);

const getNamespacesAllNames = state => getNamespaces(state).allNames;

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
  getIsFetchingNamespaces,
  getIsFetchingMetrics,
  getIsFetchingDescriptions,
  getSelectedNamespace,
  getSelectedMetric,
  getNamespacesAllNames,
  getMetricsByNamespace,
  getMetricDescription,
};
