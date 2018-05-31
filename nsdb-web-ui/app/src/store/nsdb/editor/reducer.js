import shortid from 'shortid';
import { types as TYPES } from './types';

const initialState = {
  tabs: [
    {
      id: shortid.generate(),
      title: 'New Query',
      selectedDatabase: null,
      selectedNamespace: null,
      selectedMetric: null,
    },
  ],
  databases: {
    allNames: [],
    byName: {},
  },
  namespaces: {
    allNames: [],
    byName: {},
  },
  descriptions: {
    allNames: [],
    byName: {},
  },
  isFetchingDatabases: false,
  isFetchingNamespaces: false,
  isFetchingMetrics: false,
  isFetchingDescriptions: false,
};

function editorReducer(state = initialState, action) {
  switch (action.type) {
    /**
     * normal reducers
     */
    case TYPES.TAB_ADD: {
      const { id, title } = action.payload;
      return {
        ...state,
        tabs: state.tabs.concat({
          id,
          title,
          selectedDatabase: null,
          selectedNamespace: null,
          selectedMetric: null,
        }),
      };
    }
    case TYPES.TAB_REMOVE: {
      const { id } = action.payload;
      return state.tabs.length > 1
        ? { ...state, tabs: state.tabs.filter(tab => tab.id !== id) }
        : state;
    }
    case TYPES.DATABASE_SELECT: {
      const { id, database } = action.payload;
      const newTabs = state.tabs.map((tab, index) => {
        if (tab.id === id) {
          return { ...tab, selectedDatabase: database };
        }
        return tab;
      });
      return { ...state, tabs: newTabs };
    }
    case TYPES.NAMESPACE_SELECT: {
      const { id, namespace } = action.payload;
      const newTabs = state.tabs.map((tab, index) => {
        if (tab.id === id) {
          return { ...tab, selectedNamespace: namespace };
        }
        return tab;
      });
      return { ...state, tabs: newTabs };
    }
    case TYPES.METRIC_SELECT: {
      const { id, metric } = action.payload;
      const newTabs = state.tabs.map((tab, index) => {
        if (tab.id === id) {
          return { ...tab, selectedMetric: metric };
        }
        return tab;
      });
      return { ...state, tabs: newTabs };
    }
    /**
     * async reducers
     */
    case TYPES.DATABASES_FETCH_REQUEST: {
      return { ...state, isFetchingDatabases: true };
    }
    case TYPES.DATABASES_FETCH_SUCCESS: {
      const { response } = action.payload;
      const allNames = response.dbs;

      const byName = response.dbs.reduce(
        (acc, database) => ({ ...acc, [database]: { name: database, namespaces: [] } }),
        {}
      );
      const newDatabases = { allNames, byName };
      return { ...state, databases: newDatabases, isFetchingDatabases: false };
    }
    case TYPES.DATABASES_FETCH_ERROR: {
      return { ...state, isFetchingDatabases: false };
    }
    case TYPES.NAMESPACES_FETCH_REQUEST: {
      return { ...state, isFetchingNamespaces: true };
    }
    case TYPES.NAMESPACES_FETCH_SUCCESS: {
      const { database, response } = action.payload;

      const newDatabaseWithNamespaces = {
        ...state.databases.byName[database],
        namespaces: response.namespaces,
      };
      return {
        ...state,
        databases: {
          ...state.databases,
          byName: { ...state.databases.byName, [database]: newDatabaseWithNamespaces },
        },
        isFetchingNamespaces: false,
      };
    }
    case TYPES.NAMESPACES_FETCH_ERROR: {
      return { ...state, isFetchingNamespaces: false };
    }
    case TYPES.METRICS_FETCH_REQUEST: {
      return { ...state, isFetchingMetrics: true };
    }
    case TYPES.METRICS_FETCH_SUCCESS: {
      const { namespace, response } = action.payload;
      const newNamespaceWithMetrics = {
        ...state.namespaces.byName[namespace],
        metrics: response.metrics,
      };
      return {
        ...state,
        namespaces: {
          ...state.namespaces,
          byName: { ...state.namespaces.byName, [namespace]: newNamespaceWithMetrics },
        },
        isFetchingMetrics: false,
      };
    }
    case TYPES.METRICS_FETCH_ERROR: {
      return { ...state, isFetchingMetrics: false };
    }
    case TYPES.METRIC_DESCRIPTION_FETCH_REQUEST: {
      return { ...state, isFetchingDescriptions: true };
    }
    case TYPES.METRIC_DESCRIPTION_FETCH_SUCCESS: {
      const { namespace, metric, response } = action.payload;
      const key = `${namespace}/${metric}`;
      const allNames = state.descriptions.byName[key]
        ? state.descriptions.allNames
        : state.descriptions.allNames.concat(key);
      const byName = {
        ...state.descriptions.byName,
        [key]: { name: key, fields: response.fields },
      };
      const newDescriptions = { allNames, byName };
      return { ...state, descriptions: newDescriptions, isFetchingDescriptions: false };
    }
    case TYPES.METRIC_DESCRIPTION_FETCH_ERROR: {
      return { ...state, isFetchingDescriptions: false };
    }
    default:
      return state;
  }
}

export default editorReducer;
