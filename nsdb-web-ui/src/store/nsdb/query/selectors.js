import get from 'lodash/get';

const emptyRecords = [];

/**
 * normal selectors
 */

const getResults = state => state.nsdb.query.results;

/**
 * computed selectors
 */

const getResultById = (state, props) => getResults(state).byId[props.id];

const getRecords = (state, props) => get(getResultById(state, props), 'records', emptyRecords);

export const selectors = {
  getRecords,
};
